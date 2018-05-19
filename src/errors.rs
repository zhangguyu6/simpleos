use std::convert::From;
use std::error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Allocatefail(String),
    Bucketfail(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Allocatefail(ref string) => write!(f, "Allocate fail: {}", string),
            Error::Bucketfail(ref string) => write!(f, "Bucket fail: {}", string),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::Io(ref err) => err.description(),
            Error::Allocatefail(..) => "Allocate fail",
            Error::Bucketfail(..) => "Bucket fai",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}
