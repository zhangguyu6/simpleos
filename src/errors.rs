use std::convert::From;
use std::error;
use std::fmt;
use std::io;
use std::time::SystemTimeError;
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Allocatefail(String),
    Bucketfail(String),
    InvalidFileId(String),
    InvalidKey(String),
    SystemTimeError(SystemTimeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Allocatefail(ref string) => write!(f, "Allocate fail: {}", string),
            Error::Bucketfail(ref string) => write!(f, "Bucket fail: {}", string),
            Error::InvalidFileId(ref string) => write!(f, "Invalid FileId: {}", string),
            Error::SystemTimeError(ref err) => write!(f, "Time error: {}", err),
            Error::InvalidKey(ref string) => write!(f,"Invaild Key: {}",string),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<SystemTimeError> for Error {
    fn from(err: SystemTimeError) -> Error {
        Error::SystemTimeError(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::Io(ref err) => err.description(),
            Error::SystemTimeError(ref err) => err.description(),
            Error::Allocatefail(..) => "Allocate fail",
            Error::Bucketfail(..) => "Bucket fai",
            Error::InvalidFileId(..) => "InvalidFileId",
            Error::InvalidKey(..) => "InvalidKey",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            Error::Io(ref err) => Some(err),
            Error::SystemTimeError(ref err) => Some(err),
            _ => None,
        }
    }
}
