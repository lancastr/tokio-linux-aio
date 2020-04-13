// #![deny(missing_docs, missing_debug_implementations, bare_trait_objects)]

use std::io;
use std::os::unix::prelude::*;
use std::ptr;
use std::sync::Arc;

use futures::{FutureExt, pin_mut, select, StreamExt};
use futures::channel::oneshot;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

pub use file::AioFile;
pub use mlock::LockedBuf;

use crate::errors::ContextError;
use crate::eventfd::EventFd;
use crate::requests::Request;
use crate::requests::Requests;

mod aio;
mod atomic_link;
mod eventfd;
mod mlock;
mod file;
mod wait_future;
mod requests;
mod errors;

#[derive(Copy, Clone, Debug)]
pub enum SyncLevel {
    None = 0,
    Data = aio::RWF_DSYNC as isize,
    Full = aio::RWF_SYNC as isize,
}

#[derive(Copy, Clone, Debug)]
pub enum Opcode {
    Fdsync,
    Fsync,
    Pwrite,
    Pread,
}

impl Opcode {
    fn aio_const(&self) -> u32 {
        use Opcode::*;

        match *self {
            Fdsync => aio::IOCB_CMD_FDSYNC,
            Fsync => aio::IOCB_CMD_FSYNC,
            Pwrite => aio::IOCB_CMD_PWRITE,
            Pread => aio::IOCB_CMD_PREAD,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Command {
    pub opcode: u32,
    pub offset: u64,
    pub buf: u64,
    pub len: u64,
    pub flags: u32,
}

type AioResult = aio_bindings::__s64;

pub struct AioContextInner {
    context: aio::aio_context_t,
    eventfd: RawFd,
    capacity: Semaphore,
    requests: parking_lot::Mutex<Requests>,
    _stop_tx: oneshot::Sender<()>,
}

impl AioContextInner {
    fn new(
        eventfd: RawFd,
        nr: usize,
        stop_tx: oneshot::Sender<()>,
    ) -> Result<AioContextInner, ContextError> {
        let mut context: aio::aio_context_t = 0;

        unsafe {
            if aio::io_setup(nr as libc::c_long, &mut context) != 0 {
                return Err(ContextError::IoSetup(io::Error::last_os_error()));
            }
        };

        Ok(AioContextInner {
            context,
            requests: Mutex::new(Requests::new(nr)?),
            capacity: Semaphore::new(nr),
            eventfd,
            _stop_tx: stop_tx,
        })
    }
}

impl Drop for AioContextInner {
    fn drop(&mut self) {
        let result = unsafe { aio::io_destroy(self.context) };
        assert_eq!(0, result, "io_destroy returned bad code");
    }
}

#[derive(Clone)]
pub struct AioContext {
    inner: Arc<AioContextInner>,
}

impl AioContext {
    pub fn new(nr: usize) -> Result<AioContext, ContextError> {
        let mut eventfd = EventFd::create(0, false)?;
        let (stop_tx, stop_rx) = oneshot::channel();

        let inner = Arc::new(AioContextInner::new(eventfd.as_raw_fd(), nr, stop_tx)?);

        let context = inner.context;

        let poll_future = {
            let inner = inner.clone();

            async move {
                let mut events = Vec::with_capacity(nr);

                while let Some(Ok(available)) = eventfd.next().await {
                    assert!(available > 0, "kernel reported zero ready events");
                    assert!(
                        available <= nr as u64,
                        "kernel reported more events than number of maximum tasks"
                    );

                    unsafe {
                        let num_received = aio::io_getevents(
                            context,
                            available as libc::c_long,
                            available as libc::c_long,
                            events.as_mut_ptr(),
                            ptr::null_mut::<aio::timespec>(),
                        );

                        if num_received < 0 {
                            return Err(io::Error::last_os_error());
                        }

                        assert!(
                            num_received == available as i64,
                            "io_getevents received events num not equal to reported through eventfd"
                        );
                        events.set_len(available as usize);
                    };

                    for event in &events {
                        let ptr = event.data as usize as *mut Request;

                        let sent_succeeded = unsafe { &*ptr }
                            .send_to_waiter(event.res);

                        if !sent_succeeded {
                            inner.requests.lock().return_outstanding_to_ready(ptr);
                            inner.capacity.add_permits(1)
                        }
                    }
                }

                Ok(())
            }
        }
            .fuse();

        tokio::spawn(async move {
            pin_mut!(poll_future);

            select! {
                _ = poll_future => {},
                _ = stop_rx.fuse() => {},
            }
        });

        Ok(AioContext { inner })
    }

    pub fn available_slots(&self) -> usize {
        self.inner.capacity.available_permits()
    }
}
