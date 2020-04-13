use std::io;
use std::mem::ManuallyDrop;

use memmap::MmapMut;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MemLockError {
    #[error("map_anon error: `{0}`")]
    MapAnon(#[from] io::Error),

    #[error("mlock error: `{0}`")]
    MemLock(#[from] region::Error),
}

pub struct LockedBuf {
    bytes: ManuallyDrop<MmapMut>,
    mlock_gaurd: ManuallyDrop<region::LockGuard>,
}

impl LockedBuf {
    pub fn with_capacity(cap: usize) -> Result<LockedBuf, MemLockError> {
        let bytes = MmapMut::map_anon(cap)?;
        let mlock_gaurd = region::lock(bytes.as_ref().as_ptr(), cap)?;

        Ok(LockedBuf {
            bytes: ManuallyDrop::new(bytes),
            mlock_gaurd: ManuallyDrop::new(mlock_gaurd),
        })
    }
}

impl AsRef<[u8]> for LockedBuf {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl AsMut<[u8]> for LockedBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.bytes.as_mut()
    }
}

impl Drop for LockedBuf {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.mlock_gaurd);
            ManuallyDrop::drop(&mut self.bytes);
        }
    }
}
