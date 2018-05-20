use data::{Record, RecordWriter,Recordfile};
use errors::Error;
use filepool::{FilePool,max_filesize};
use std::collections::{BTreeMap, HashSet,VecDeque};
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use util::{get_timestamp, Timestamp,roundup};

// FIXME 应该可以被配置
const ratio:f32 = 0.75;


#[derive(Debug)]
struct Log<'a> {
    // datafile的句柄池
    filepool: Arc<Mutex<FilePool>>,
    // 待同步文件列表
    syncpool: HashSet<Timestamp>,
    // key-offset索引
    indexmap: BTreeMap<Vec<u8>, Slot>,
    // 代写的indexfile列表
    writer: RecordWriter<'a>,
}

impl<'a> Log<'a> {
    fn new(datafilepool: Arc<Mutex<FilePool>>) -> Log<'a>
where {
        Log {
            filepool: datafilepool.clone(),
            syncpool: HashSet::new(),
            indexmap: BTreeMap::new(),
            writer: RecordWriter::new(datafilepool.clone()),
        }
    }
    // 得到record
    fn get_record<K>(&mut self, key: &K) -> Result<Option<Record<'a>>, Error>
    where
        K: AsRef<[u8]>,
    {
        let slot = self.indexmap.get(key.as_ref());
        if let Some(_slot) = slot {
            let mut file = self.filepool.lock().unwrap().get_file(_slot.fileid)?;
            file.seek(SeekFrom::Start(_slot.offset as u64));
            let record = Record::read_from(&mut file)?;
            if record.is_none() {
                Err(Error::InvalidKey("key in map but not in disk".to_string()))
            } else {
                Ok(record)
            }
        } else {
            Ok(None)
        }
    }
    // 设置key,存在则先删除再追加
    fn get_value<K>(&mut self, key: &K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        match self.get_record(key)? {
            None => Ok(None),
            Some(record) => Ok(Some(Vec::from(record.value))),
        }
    }
    fn set<K, V>(&mut self, key: K, value: V, write_now: bool, sync_now: bool) -> Result<(), Error>
    where
        Vec<u8>: From<K>,
        Vec<u8>: From<V>,
    {
        let keyvec = Vec::from(key);
        match self.indexmap.get(&keyvec) {
            None => {}
            Some(_) => {
                self.remove(&keyvec, write_now, sync_now)?;
            }
        }
        self.append(keyvec, value, write_now, sync_now)
    }

    // 追加record
    fn append<K, V>(
        &mut self,
        key: K,
        value: V,
        write_now: bool,
        sync_now: bool,
    ) -> Result<(), Error>
    where
        Vec<u8>: From<K>,
        Vec<u8>: From<V>,
    {
        let keyvec = Vec::from(key);
        let valvec = Vec::from(value);
        let time = get_timestamp()?;
        let record = Record::new(keyvec.clone(), valvec, time);
        // 获取追加位置,调整lastfileid及其freelist
        let (fileid, offset) = self.writer.get_offset(&record)?;
        let newslot = Slot::new(offset, fileid, time);
        // 插入record
        self.writer.insert_record(fileid, offset, record)?;
        // 写记录
        if write_now {
            self.writer.write_all(sync_now)?;
        }
        // 加入内存中的btree
        self.indexmap.insert(keyvec, newslot);
        // 加入同步池
        self.syncpool.insert(fileid);
        Ok(())
    }
    // 删除record
    fn remove<K>(
        &mut self,
        key: &K,
        write_now: bool,
        sync_now: bool,
    ) -> Result<Option<Record<'a>>, Error>
    where
        K: AsRef<[u8]>,
    {
        match self.get_record(key)? {
            None => Ok(None),
            Some(record) => {
                // 使用原slot位置为写位置
                let slot = self.indexmap.get(key.as_ref()).unwrap().clone();
                let keyvec = vec![0; record.key.len()];
                let valvec = vec![0; record.value.len()];
                let delrecord = Record::new(keyvec, valvec, 0);
                // 插入空record
                self.writer
                    .insert_record(slot.fileid, slot.offset, delrecord)?;
                // 写记录
                if write_now {
                    self.writer.write_all(sync_now)?;
                }
                // 释放recod空间
                self.writer.free_record(&record, slot.fileid, slot.offset);
                // 删除内存中的btree
                self.indexmap.remove(key.as_ref());
                // 加入同步池
                self.syncpool.insert(slot.fileid);
                Ok(Some(record))
            }
        }
    }

    // 同步
    fn sync_all(&mut self) -> Result<(), Error> {
        for fileid in self.syncpool.drain() {
            let file = self.filepool.lock().unwrap().get_file(fileid)?;
            file.sync_all()?;
        }
        Ok(())
    }
    // 压缩
    fn compress(&mut self) -> Result<(),Error> {
        let filelist = self.filepool.lock().unwrap().compress_filelist(ratio)?;
        let mut realloclist:VecDeque<(u64,u32)> = VecDeque::new();
        for fileid in filelist{
            // 删除索引文件
            self.filepool.lock().unwrap().removefile_withid(fileid,false)?;
            // 已用大小,文件句柄
            let  (endoff, file) = self.filepool.lock().unwrap().get_fileandfree(fileid)?;
            let mut file = file;
            realloclist.push_back((fileid,0));
            // 记录文件
            let recordfile = Recordfile::read_from(&mut file, fileid, endoff)?;
            // 可写文件id,可写文件目前最大偏移
            let (reallocfileid,offset) = realloclist.pop_front().unwrap();
            let mut writefileid = reallocfileid;
            let mut writeoffset = offset;
            for record in recordfile.records {
                if writeoffset + roundup(record.size(),16) as u32 >= max_filesize{
                    let file_offset = realloclist.pop_front().unwrap();
                    writefileid = file_offset.0; 
                    writeoffset = file_offset.1;
                }
                self.writer.insert_record(writefileid, writeoffset, record)?;
            }
            if writeoffset < max_filesize {
                realloclist.push_front((writefileid,writeoffset));
            }
            // 先写index
            self.writer.write_allIndex(true)?;
            // 后写record
            self.writer.write_all(true)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Slot {
    fileid: u64,
    offset: u32,
    time: Timestamp,
}
impl Slot {
    fn new(offset: u32, fileid: u64, time: Timestamp) -> Slot
where {
        Slot {
            fileid: fileid,
            offset: offset,
            time: time,
        }
    }
}
