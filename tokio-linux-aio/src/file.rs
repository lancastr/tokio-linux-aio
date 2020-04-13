use std::{io, mem, ptr};
use std::os::unix::prelude::*;
use std::path::Path;

use futures::channel::oneshot;

use crate::{AioContext, AioResult, Command, Opcode, SyncLevel};
use crate::aio;
use crate::errors::{AioCommandError, AioFileError};
use crate::requests::Request;
use crate::wait_future::AioWaitFuture;

pub struct AioFile {
    fd: RawFd,
}

impl AsRawFd for AioFile {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl FromRawFd for AioFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        AioFile { fd }
    }
}

impl Drop for AioFile {
    fn drop(&mut self) {
        nix::unistd::close(self.fd).expect("Error closing AIO file");
    }
}

impl AioFile {
    pub async fn submit_request(
        &self,
        aio: &AioContext,
        command: Command,
    ) -> Result<AioResult, AioCommandError> {
        let inner_context = aio.inner.clone();

        inner_context.capacity.acquire().await.forget();

        let mut request = inner_context.requests.lock().take();

        let request_addr = Request::aio_addr(&request);

        let (tx, rx) = oneshot::channel();
        let base;

        {
            let mut request_ptr_array: [*mut aio::iocb; 1] = [ptr::null_mut(); 1];

            request
                .set_payload(
                    &mut request_ptr_array,
                    request_addr,
                    inner_context.eventfd,
                    self.fd,
                    command,
                    tx
                );

            base = AioWaitFuture::new(inner_context.clone(), rx, request);

            let iocb_ptr = request_ptr_array.as_mut_ptr() as *mut *mut aio::iocb;

            let result = unsafe { aio::io_submit(inner_context.context, 1, iocb_ptr) };

            if result != 1 {
                return Err(AioCommandError::IoSubmit(io::Error::last_os_error()));
            }
        }

        let code = base.await?;

        if code < 0 {
            Err(AioCommandError::BadResult(io::Error::from_raw_os_error(
                -code as _,
            )))
        } else {
            Ok(code)
        }
    }

    pub fn create<P: AsRef<Path>>(path: P) -> Result<AioFile, AioFileError> {
        let fd = nix::fcntl::open(
            path.as_ref(),
            nix::fcntl::OFlag::O_DIRECT | nix::fcntl::OFlag::O_RDWR | nix::fcntl::OFlag::O_CREAT,
            nix::sys::stat::Mode::empty(),
        )?;
        Ok(AioFile { fd })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<AioFile, AioFileError> {
        let fd = nix::fcntl::open(
            path.as_ref(),
            nix::fcntl::OFlag::O_DIRECT | nix::fcntl::OFlag::O_RDWR,
            nix::sys::stat::Mode::empty(),
        )?;

        Ok(AioFile { fd })
    }

    pub async fn read(
        &self,
        aio: &AioContext,
        offset: u64,
        buffer: &mut [u8],
    ) -> Result<AioResult, AioCommandError> {
        let (ptr, len) = {
            let len = buffer.len() as u64;
            let ptr = unsafe { mem::transmute::<_, usize>(buffer.as_ptr()) } as u64;
            (ptr, len)
        };

        self
            .submit_request(
                aio,
                Command {
                    opcode: Opcode::Pread.aio_const(),
                    offset,
                    len,
                    buf: ptr,
                    flags: 0,
                },
            )
            .await
    }

    pub async fn write(
        &self,
        aio: &AioContext,
        offset: u64,
        buffer: &[u8],
    ) -> Result<AioResult, AioCommandError> {
        self.write_sync(aio, offset, buffer, SyncLevel::None).await
    }

    pub async fn write_sync(
        &self,
        aio: &AioContext,
        offset: u64,
        buffer: &[u8],
        sync_level: SyncLevel,
    ) -> Result<AioResult, AioCommandError> {
        let (ptr, len) = {
            let len = buffer.len() as u64;
            let ptr = unsafe { mem::transmute::<_, usize>(buffer.as_ptr()) } as libc::c_long;
            (ptr, len)
        };

        self
            .submit_request(
                aio,
                Command {
                    opcode: Opcode::Pwrite.aio_const(),
                    offset,
                    len,
                    buf: ptr as u64,
                    flags: sync_level as u32,
                },
            )
            .await
    }

    pub async fn sync(&self, aio: &AioContext) -> Result<AioResult, AioCommandError> {
        self
            .submit_request(
                aio,
                Command {
                    opcode: Opcode::Fsync.aio_const(),
                    buf: 0,
                    len: 0,
                    offset: 0,
                    flags: 0,
                },
            )
            .await
    }

    pub async fn data_sync(&self, aio: &AioContext) -> Result<AioResult, AioCommandError> {
        self
            .submit_request(
                aio,
                Command {
                    opcode: Opcode::Fdsync.aio_const(),
                    buf: 0,
                    len: 0,
                    offset: 0,
                    flags: 0,
                },
            ).await
    }
}