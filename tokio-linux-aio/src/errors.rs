use std::io;

use thiserror::Error;

use crate::eventfd::EventFdError;

#[derive(Error, Debug)]
pub enum AioCommandError {
    #[error("AioContext stopped")]
    AioStopped,

    #[error("io_submit error: {0}")]
    IoSubmit(io::Error),

    #[error("bad error: `{0}`")]
    BadResult(io::Error),
}


#[derive(Error, Debug)]
pub enum ContextError {
    #[error("EventFd error: `{0}`")]
    EventFd(#[from] EventFdError),

    #[error("IoSetup error: `{0}`")]
    IoSetup(#[from] io::Error),
}


//TODO: merge with AioCommandError ?
#[derive(Error, Debug)]
pub enum AioFileError {
    #[error("system error: `{0}`")]
    System(#[from] nix::Error),
}
