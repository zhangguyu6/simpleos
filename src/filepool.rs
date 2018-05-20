use errors::Error;
use freelist::FreeList;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::vec::Vec;
use util::Timestamp;
use util::get_timestamp;

// 最大文件大小为32M
pub const max_filesize: u32 = 1 << 25;
// 最大文件句柄数为16
const max_filehandler: usize = 16;

#[derive(Debug)]
pub struct FilePool {
    // data文件句柄词
    datafile_pool: HashMap<Timestamp, (FreeList, Vec<File>)>,
    // 目录路径
    dirpath: PathBuf,
    // 当前活跃文件id
    lastfileid: Timestamp,
}

impl FilePool {
    pub fn new<'a, P>(dirpathstr: P) -> Result<FilePool, Error>
    where
        P: Into<&'a str>,
    {
        let dirpath = Path::new(dirpathstr.into());
        let mut datafile_pool = HashMap::new();
        let mut lastfileid: u64 = 0;

        for entry in fs::read_dir(dirpath)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.ends_with(".data") {
                let fileid = path.file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if fileid > lastfileid {
                    lastfileid = fileid;
                }
                datafile_pool.insert(
                    fileid,
                    (
                        FreeList::new(max_filesize),
                        Vec::with_capacity(max_filehandler),
                    ),
                );
            }
        }
        let mut filepool = FilePool {
            dirpath: PathBuf::from(dirpath),
            datafile_pool: datafile_pool,
            lastfileid: lastfileid,
        };
        if filepool.datafile_pool.is_empty() {
            let time = get_timestamp()?;
            let file = filepool.createfile_withid(time)?;
            filepool
                .datafile_pool
                .insert(time, (FreeList::new(max_filesize), vec![file]));
        }
        Ok(filepool)
    }
    // 从文件池中返回句柄
    pub fn get_file(&mut self, fileid: u64) -> Result<File, Error> {
        match self.datafile_pool.get_mut(&fileid) {
            Some((_, filelist)) => Ok(filelist.pop().unwrap_or(self.openfile_withid(fileid)?)),
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }
    // 打开索引文件
    pub fn get_indexfile(&self, fileid: u64) -> Result<File, Error> {
        match self.datafile_pool.get(&fileid) {
            Some(..) => Ok(self.getindexfile_withid(fileid)?),
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }

    // 从文件池中返回句柄的最大偏移
    pub fn get_fileandfree(&mut self, fileid: u64) -> Result<(u32, File), Error> {
        match self.datafile_pool.get_mut(&fileid) {
            Some((freelist, filelist)) => {
                let endoff = freelist.get_usedfilesize();
                Ok((
                    endoff,
                    filelist.pop().unwrap_or(self.openfile_withid(fileid)?),
                ))
            }
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }

    // 将句柄放回文件池
    pub fn put_file(&mut self, fileid: u64, file: File) -> Result<(), Error> {
        match self.datafile_pool.get_mut(&fileid) {
            Some((_, filelist)) => {
                if max_filehandler > filelist.len() {
                    filelist.push(file);
                };
                Ok(())
            }
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }

    // 得到最新的活跃文件id
    pub fn get_lastfileid(&self) -> Timestamp {
        self.lastfileid
    }
    // 得到最新的活跃文件
    pub fn get_lastfile(&mut self) -> Result<File, Error> {
        let lastfileid = self.lastfileid;
        self.get_file(lastfileid)
    }
    // 释放record空间
    pub fn free_room(&mut self, size: u32, offset: u32, fileid: Timestamp) -> Result<(), Error> {
        match self.datafile_pool.get_mut(&fileid) {
            Some((freelist, _)) => Ok(freelist.free_room(offset, size)?),
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }

    // 根据size得到目标文件的偏移
    pub fn request_room_withid(&mut self, size: u32, fileid: Timestamp) -> Result<u32, Error> {
        match self.datafile_pool.get_mut(&fileid) {
            Some((freelist, _)) => Ok(freelist.request_room(size)?),
            None => Err(Error::InvalidFileId("fileid not in file pool".to_string())),
        }
    }
    // 根据size得到最新文件的偏移,空间不够则新建文件
    pub fn request_room_ornew(&mut self, size: u32) -> Result<(Timestamp, u32), Error> {
        let lastfileid = self.lastfileid;
        match self.request_room_withid(size, lastfileid) {
            Ok(off) => Ok((lastfileid, off)),
            Err(Error::Allocatefail(..)) => {
                let time: Timestamp = get_timestamp()?;
                let mut freelist = FreeList::new(max_filesize);
                let mut filelist = Vec::with_capacity(max_filehandler);
                self.lastfileid = time;
                let off = freelist.request_room(size)?;
                filelist.push(self.createfile_withid(time)?);
                self.datafile_pool
                    .insert(self.lastfileid, (freelist, filelist));
                Ok((self.lastfileid, off))
            }
            Err(err) => Err(err),
        }
    }
    // 返回所有应当压缩的文件列表
    pub fn compress_filelist(&self, ratio: f32) -> Result<Vec<Timestamp>, Error> {
        let mut fileidlists = Vec::new();
        for (fileid, (freelist, _)) in self.datafile_pool.iter() {
            let occupyratio =
                (freelist.get_compfilesize() as f64 / freelist.get_usedfilesize() as f64) as f32;
            if occupyratio < ratio && *fileid != self.lastfileid {
                fileidlists.push(*fileid);
            }
        }
        Ok(fileidlists)
    }

    pub fn openfile_withid(&self, fileid: u64) -> Result<File, Error> {
        let mut file_pathbuf = PathBuf::from(fileid.to_string());
        file_pathbuf.set_extension(".data");
        let path = self.dirpath.join(file_pathbuf.as_path());
        Ok(OpenOptions::new().read(true).write(true).open(path)?)
    }

    pub fn createfile_withid(&self, fileid: u64) -> Result<File, Error> {
        let mut file_pathbuf = PathBuf::from(fileid.to_string());
        file_pathbuf.set_extension(".data");
        let path = self.dirpath.join(file_pathbuf.as_path());
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?)
    }

    pub fn getindexfile_withid(&self, fileid: u64) -> Result<File, Error> {
        let mut file_pathbuf = PathBuf::from(fileid.to_string());
        file_pathbuf.set_extension(".index");
        let path = self.dirpath.join(file_pathbuf.as_path());
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?)
    }

    pub fn removefile_withid(&self,fileid:u64,isdata:bool) -> Result<(),Error> {
        let mut file_pathbuf = PathBuf::from(fileid.to_string());
        if isdata {
            file_pathbuf.set_extension(".data");
        }
        else {
            file_pathbuf.set_extension(".index");
        }
        let path = self.dirpath.join(file_pathbuf.as_path());
        fs::remove_file(path)?;
        Ok(())
    }
}
