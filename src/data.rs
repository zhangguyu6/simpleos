use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use errors::Error;
use filepool::FilePool;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use util::{roundup, Timestamp};
// .data 文件中的记录结构
// key和value的应当大于u32
#[derive(Debug, Clone)]
pub struct Record<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, [u8]>,
    pub time: Timestamp,
}

impl<'a> Record<'a> {
    pub fn new<K, V>(key: K, value: V, time: Timestamp) -> Record<'a>
    where
        Cow<'a, [u8]>: From<K>,
        Cow<'a, [u8]>: From<V>,
    {
        let key = Cow::from(key);
        let value = Cow::from(value);
        Record {
            key: key,
            value: value,
            time: time,
        }
    }
    pub fn size(&self) -> usize {
        2 + 4 + 8 + self.key.len() + self.value.len()
    }

    pub fn read_from<R>(reader: &mut R) -> Result<Option<Record<'a>>, Error>
    where
        R: Read + Seek,
    {
        let keysize = reader.read_u16::<LittleEndian>()?;
        if keysize == 0 {
            return Ok(None);
        }
        let valuesize = reader.read_u32::<LittleEndian>()?;
        let time = reader.read_u64::<LittleEndian>()?;
        let mut keybuf = vec![0; keysize as usize];
        reader.read_exact(&mut keybuf)?;
        let mut valuebuf = vec![0; valuesize as usize];
        reader.read_exact(&mut valuebuf)?;
        let key = Cow::from(keybuf);
        let value = Cow::from(valuebuf);
        Ok(Some(Record {
            key: key,
            value: value,
            time: time,
        }))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let allocsize = roundup(self.size(), 16);
        let mut buf = Cursor::new(Vec::with_capacity(allocsize));
        buf.write_u16::<LittleEndian>(self.key.len() as u16)?;
        buf.write_u32::<LittleEndian>(self.value.len() as u32)?;
        buf.write_u64::<LittleEndian>(self.time)?;
        buf.write_all(&self.key)?;
        buf.write_all(&self.value)?;
        Ok(buf.into_inner())
    }
    fn write_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write + Seek,
    {
        let buf: Vec<u8> = self.to_bytes()?;
        writer.write_all(&buf)?;
        Ok(())
    }
}
#[derive(Debug)]
pub struct Recordfile<'a> {
    pub fileid: Timestamp,
    pub size: u32,
    pub records: Vec<Record<'a>>,
}
impl<'a> Recordfile<'a> {
    pub fn read_from<R>(
        reader: &mut R,
        fileid: Timestamp,
        endoff: u32,
    ) -> Result<Recordfile<'a>, Error>
    where
        R: Read + Seek,
    {
        let mut records: Vec<Record<'a>> = Vec::new();
        let mut off = 0;
        let mut size = 0;
        loop {
            let rec = Record::read_from(reader)?;
            match rec {
                None => off += 2,
                Some(record) => {
                    let recsize = record.size();
                    let allocsize = roundup(recsize, 16);
                    off += allocsize;
                    size += allocsize;
                    // 跳过分配距离
                    reader.seek(SeekFrom::Start(off as u64))?;
                    records.push(record);
                }
            }
            if off >= endoff as usize {
                break;
            }
        }
        Ok(Recordfile {
            fileid: fileid,
            size: size as u32,
            records: records,
        })
    }
}

#[derive(Debug)]
pub struct RecordWriter<'a> {
    // datafile的句柄池
    filepool: Arc<Mutex<FilePool>>,
    // 所有待写Record的hashmap
    recordmap: HashMap<u64, Vec<(u32, Record<'a>)>>,
}

impl<'a> RecordWriter<'a> {
    pub fn new(filepool: Arc<Mutex<FilePool>>) -> RecordWriter<'a> {
        RecordWriter {
            filepool: filepool,
            recordmap: HashMap::new(),
        }
    }
    // 得到待写文件的偏移
    pub fn get_offset(&mut self, record: &Record) -> Result<(u64, u32), Error> {
        let size = roundup(record.size(), 16);
        let (fileid, offset) = self.filepool
            .lock()
            .unwrap()
            .request_room_ornew(size as u32)?;
        Ok((fileid, offset))
    }
    // 释放记录文件空间
    pub fn free_record(
        &mut self,
        record: &Record,
        fileid: Timestamp,
        offset: u32,
    ) -> Result<(), Error> {
        let size = roundup(record.size(), 16);
        self.filepool
            .lock()
            .unwrap()
            .free_room(size as u32, offset, fileid)
    }
    // 插入一个待写record
    // FIXME:ERROR
    pub fn insert_record(
        &mut self,
        fileid: u64,
        offset: u32,
        record: Record<'a>,
    ) -> Result<(), Error> {
        if self.recordmap.contains_key(&fileid) {
            self.recordmap.insert(fileid, vec![(offset, record)]);
        } else {
            self.recordmap
                .get_mut(&fileid)
                .unwrap()
                .push((offset, record));
        }
        Ok(())
    }
    // 写全部map中的记录,并将map清空
    pub fn write_all(&mut self, sync_now: bool) -> Result<(), Error> {
        for (fileid, recordlist) in self.recordmap.drain() {
            let mut file = self.filepool.lock().unwrap().get_file(fileid)?;
            for (offset, record) in recordlist.iter() {
                file.seek(SeekFrom::Start(*offset as u64))?;
                record.write_bytes(&mut file)?;
                if sync_now {
                    file.sync_all()?;
                }
            }
        }
        Ok(())
    }
    // 根据map的记录,写索引
    pub fn write_allIndex(&mut self, sync_now: bool) -> Result<(), Error> {
        for (fileid, recordlist) in self.recordmap.iter() {
            let mut file = self.filepool.lock().unwrap().get_indexfile(*fileid)?;
            for (offset, record) in recordlist.iter() {
                let index = Index::new(record, *offset);
                index.write_bytes(&mut file)?;
                if sync_now {
                    file.sync_all()?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Index {
    keysize: u16,
    valuesize: u32,
    offset: u32,
    time: Timestamp,
}
impl Index {
    fn new(record: &Record, offset: u32) -> Index
where {
        let keysize = record.key.len();
        let valuesize = record.value.len();
        let time = record.time;
        Index {
            keysize: keysize as u16,
            valuesize: valuesize as u32,
            offset: offset,
            time: time,
        }
    }
    #[inline]
    fn size(&self) -> usize {
        2 + 4 + 4 + 8
    }
    // 从indexfile中读取slot
    fn read_from<R>(reader: &mut R) -> Result<Option<Index>, Error>
    where
        R: Read + Seek,
    {
        let keysize = reader.read_u16::<LittleEndian>()?;
        let valuesize = reader.read_u32::<LittleEndian>()?;
        if keysize == 0 || valuesize == 0 {
            return Ok(None);
        }
        let mut keybuf = vec![0; keysize as usize];
        reader.read_exact(&mut keybuf)?;
        let offset = reader.read_u32::<LittleEndian>()?;
        let time = reader.read_u64::<LittleEndian>()?;
        Ok(Some(Index {
            keysize: keysize,
            valuesize: valuesize,
            offset: offset,
            time: time,
        }))
    }
    // slot转化为vec<u8>
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Cursor::new(Vec::with_capacity(self.size()));
        buf.write_u16::<LittleEndian>(self.keysize)?;
        buf.write_u32::<LittleEndian>(self.valuesize)?;
        buf.write_u32::<LittleEndian>(self.offset)?;
        buf.write_u64::<LittleEndian>(self.time)?;
        Ok(buf.into_inner())
    }
    // slot写index文件
    fn write_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write + Seek,
    {
        let buf: Vec<u8> = self.to_bytes()?;
        writer.write_all(&buf)?;
        Ok(())
    }
}
