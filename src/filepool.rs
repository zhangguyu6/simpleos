use errors::Error;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::path::Path;

#[derive(Debug)]
pub struct FilePool<'a> {
    // 文件路径
    file_path: &'a Path,
    // 文件句柄池
    file_pool: VecDeque<File>,
}

impl<'a> FilePool<'a> {
    pub fn new<P>(filepath: P, capacity: usize) -> FilePool<'a>
    where
        P: Into<&'a str>,
    {
        FilePool {
            file_path: Path::new(filepath.into()),
            file_pool: VecDeque::with_capacity(capacity),
        }
    }
    // 从文件池中返回句柄
    pub fn get_file(&mut self) -> Result<File, Error> {
        match self.file_pool.pop_front() {
            Some(file) => Ok(file),
            None => Ok(OpenOptions::new().read(true).open(self.file_path)?),
        }
    }
    // 将句柄放回文件池
    pub fn put_file(&mut self, file: File) -> Result<(), Error> {
        if self.file_pool.capacity() > self.file_pool.len() {
            self.file_pool.push_back(file);
        }
        Ok(())
    }
}
