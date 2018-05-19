use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use errors::Error;
use filepool::FilePool;
use freelist::FreeList;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::drop;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;
use util::{get_hash, get_indexpos};

// FIXME:应当是可配置的
// index文件的最大大小
const max_filesize: u64 = 2 << 32;
// 每个bucket不溢出时的最大slot数
const slotnums: usize = 32;
// slotsize在页面中的大小
const slotsize: usize = 22;
// bucket在页面中的大小
const bucketsize: usize = slotnums * slotsize + 8 + 8 + 1;
// 空闲列表在页面中的大小
const freelistsize:usize = 2048;

#[derive(Debug)]
struct Index<'a> {
    // FIXME: rwlock+cache 考虑与index解耦
    // buckets: Mutex<Cache<u64,LinkedList<Bucket>>> ,
    // 当前bucket的最大数量为 2^(n+1) (初始为0)
    level: u32,
    // 当前split的bucket数量 (初始为0)
    split: u32,
    // indexfile的路径
    path: &'a Path,
    // indexfile的句柄池 只在read时起作用
    pool: Mutex<FilePool<'a>>,
    // indexfile的空闲空间list
    freelist: Arc<Mutex<FreeList>>,
    // datafile的空闲空间list
    datafreelist: Arc<Mutex<FreeList>>,
    // bucket的index-pos表
    bucketpos_map: HashMap<u64, u64>,
    // index的可写句柄,只在读时起作用
    writer: IndexWriter,
}

#[derive(Debug)]
struct IndexWriter {
    // indexfile的句柄
    index_handler: File,
    max_filesize: u64,
}
impl<'a> IndexWriter {
    // 从句柄池中得到一个可写的句柄
    fn new<P>(file: File) -> Result<IndexWriter, io::Error>
    where
        P: Into<&'a str>,
    {
        Ok(IndexWriter {
            index_handler: file,
            max_filesize: max_filesize,
        })
    }
}
impl Seek for IndexWriter {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        self.index_handler.seek(pos)
    }
}
impl Write for IndexWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        // 写入前进行数据溢出检查
        let current_size = self.seek(SeekFrom::End(0))?;
        if current_size + (buf.len() as u64) < self.max_filesize {
            Err(io::Error::new(io::ErrorKind::Other, "data size overflow!"))
        } else {
            self.index_handler.write(buf)
        }
    }
    fn flush(&mut self) -> Result<(), io::Error> {
        self.index_handler.flush()
    }
}

impl<'a> Index<'a> {
    fn new<P>(
        bucketcap: usize,
        path: P,
        handlercap: usize,
        freelist: Arc<Mutex<FreeList>>,
        datafreelist: Arc<Mutex<FreeList>>,
    ) -> Result<Index<'a>, Error>
    where
        P: Into<&'a str>,
    {
        let pathstr = path.into();
        let file = OpenOptions::new().read(true).write(true).open(pathstr)?;
        let mut bucketpos: HashMap<u64, u64> = HashMap::new();
        // 分配freelist的2个bucket
        for i in 0..2 {
        let freelist_pos = freelist.lock().unwrap().request_room(freelistsize as u64)?;
        bucketpos.insert(i,freelist_pos);
        }
        // 分配level0 的2个bucket
        for i in 3..5 {
            let pos = freelist.lock().unwrap().request_room(bucketsize as u64)?;
            bucketpos.insert(i, pos);

        }
        Ok(Index {
            level: 0,
            split: 0,
            path: Path::new(pathstr),
            pool: Mutex::new(FilePool::new(pathstr, handlercap)),
            freelist: freelist,
            datafreelist: datafreelist,
            bucketpos_map: bucketpos,
            writer: IndexWriter {
                index_handler: file,
                max_filesize: max_filesize,
            },
        })
    }
    fn get_slot<K>(&self, key: &K) -> Result<Option<Slot>, Error>
    where
        K: AsRef<[u8]>,
    {
        let hash = get_hash(key);
        // 前2个为freelist
        let bucketindex = get_indexpos(hash, self.level as u64, self.split as u64)+2;
        // 找到bucket在文件中位置
        let bucketpos = self.bucketpos_map.get(&bucketindex).map_or(
            Err(Error::Bucketfail("can not find bucket index".to_string())),
            Ok,
        )?;
        // 从文件池中获取文件句柄
        let mut lock = self.pool.lock().unwrap();
        let mut file = lock.get_file()?;
        drop(lock);
        // 遍历index所在的bucket及其溢出页的全部slot
        let mut bucket = Bucket::read_from(&mut file, *bucketpos)?;
        loop {
            for slot in bucket.slots {
                if slot.hash == hash {
                    return Ok(Some(slot));
                }
            }
            if let Some(bucketpos) = bucket.overflow_off {
                bucket = Bucket::read_from(&mut file, bucketpos)?;
            } else {
                break;
            }
        }
        // 将句柄放回文件池
        let mut lock = self.pool.lock().unwrap();
        lock.put_file(file)?;
        Ok(None)
    }
    // index不消耗key和value
    fn set_slot<K, V>(&mut self, key: &K, value: &V) -> Result<Slot, Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let hash = get_hash(key);
        let bucketindex = get_indexpos(hash, self.level as u64, self.split as u64)+2;
        let keysize = key.as_ref().len();
        let valuesize = value.as_ref().len();
        let mut bucketpos = *self.bucketpos_map.get(&bucketindex).map_or(
            Err(Error::Bucketfail("can not find bucket index".to_string())),
            Ok,
        )?;
        // 找到带插入slot的bucket位置和slot序号
        let mut bucket = Bucket::read_from(&mut self.writer.index_handler, bucketpos)?;
        let mut slotindex = 0;
        let (slot, slotoffset) =
            Bucket::find_slot(&mut self.writer.index_handler, bucketpos, hash)?;
        // 请求datafile的空闲位置,返回value的空闲位置
        let recordoffset = self.datafreelist
            .lock()
            .unwrap()
            .request_room((keysize + valuesize) as u64)?;

        match slot {
            Some(_slot) => {
                let mut _slot = _slot;
                _slot.offset = recordoffset;
                _slot.write_bytes(&mut self.writer, slotoffset)?;
                Ok(_slot)
            }
            None => {
                bucket = Bucket::read_from(&mut self.writer.index_handler, slotoffset)?;
                let _slot = Slot {
                    hash: hash,
                    keysize: keysize as u16,
                    valuesize: valuesize as u32,
                    offset: recordoffset,
                };
                if bucket.slots.len() == slotnums {
                    let mut newbucket = Bucket::new(bucket.index, true);
                    bucketpos = self.freelist
                        .lock()
                        .unwrap()
                        .request_room(bucketsize as u64)?;
                    newbucket.slots.push(_slot.clone());
                    bucket.overflow_off = Some(bucketpos);
                    bucket.write_bytes(&mut self.writer, slotoffset);
                    newbucket.write_bytes(&mut self.writer, bucketpos);
                } else {
                    bucket.slots.push(_slot.clone());
                    bucket.write_bytes(&mut self.writer, slotoffset);
                }
                    Ok(_slot)
            }
        }
    }
    // split index
    fn split(&self){
        
    }
}
#[derive(Debug, Clone)]
struct Slot {
    hash: u64,
    keysize: u16,
    valuesize: u32,
    offset: u64,
}
impl Slot {
    // FIXME: 应该可以配置

    // 从index_file中读取slot
    fn read_from<R>(reader: &mut R, offset: u64) -> Result<Option<Slot>, Error>
    where
        R: Read + Seek,
    {
        reader.seek(SeekFrom::Start(offset))?;
        let hash = reader.read_u64::<LittleEndian>()?;
        // 空Slot直接返回
        if hash == 0 {
            return Ok(None);
        }
        let keysize = reader.read_u16::<LittleEndian>()?;
        let valuesize = reader.read_u32::<LittleEndian>()?;
        let offset = reader.read_u64::<LittleEndian>()?;
        Ok(Some(Slot {
            hash: hash,
            keysize: keysize,
            valuesize: valuesize,
            offset: offset,
        }))
    }
    // slot转化为vec<u8>
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Cursor::new(Vec::with_capacity(slotsize));
        buf.set_position(0);
        buf.write_u64::<LittleEndian>(self.hash)?;
        buf.write_u16::<LittleEndian>(self.keysize)?;
        buf.write_u32::<LittleEndian>(self.valuesize)?;
        buf.write_u64::<LittleEndian>(self.offset)?;
        Ok(buf.into_inner())
    }
    // slot写index文件
    fn write_bytes<W>(&self, writer: &mut W, offset: u64) -> Result<(), Error>
    where
        W: Write + Seek,
    {
        let buf: Vec<u8> = self.to_bytes()?;
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[derive(Debug)]
struct Bucket {
    index: u64,
    // 是否是溢出页
    is_overflow: bool,
    // 下一个溢出页的偏移地址
    overflow_off: Option<u64>,
    slots: Vec<Slot>,
}
impl Bucket {
    fn new(index: u64, is_overflow: bool) -> Bucket {
        Bucket {
            index: index,
            is_overflow: is_overflow,
            overflow_off: None,
            slots: Vec::with_capacity(slotnums),
        }
    }

    fn read_from<R>(reader: &mut R, offset: u64) -> Result<Bucket, Error>
    where
        R: Read + Seek,
    {
        let index = reader.read_u64::<LittleEndian>()?;
        let is_overflow = if reader.read_u8()? == 0 { false } else { true };
        let _overflow_off = reader.read_u64::<LittleEndian>()?;
        let overflow_off = if _overflow_off == 0 {
            None
        } else {
            Some(_overflow_off)
        };
        // 读取slots
        let mut slots: Vec<Slot> = Vec::with_capacity(slotnums);
        let mut _offset = offset;
        for i in 0..(slotnums - 1) {
            let slot = Slot::read_from(reader, _offset)?;
            if slot.is_some() {
                slots.push(slot.unwrap());
            } else {
                // 遇到一个空slot停止
                break;
            }
        }
        Ok(Bucket {
            slots: slots,
            overflow_off: overflow_off,
            index: index,
            is_overflow: is_overflow,
        })
    }
    // 查找bucket及其所有溢出中符合hash的slot,返回其位置,否则返回溢出页的位置
    fn find_slot<R>(reader: &mut R, bucketpos: u64, hash: u64) -> Result<(Option<Slot>, u64), Error>
    where
        R: Read + Seek,
    {
        let mut bucket = Bucket::read_from(reader, bucketpos)?;
        loop {
            for i in 0..bucket.slots.len() {
                // 找到,则bucket的slot index就是要修改的序号
                if bucket.slots[i].hash == hash {
                    // 找到,返回slot的值和位置
                    return Ok((
                        Some(bucket.slots[i].clone()),
                        bucketpos + (slotsize * i) as u64,
                    ));
                }
            }
            match bucket.overflow_off {
                Some(bucketpos) => {
                    bucket = Bucket::read_from(reader, bucketpos)?;
                }
                None => {
                    // 未找到,返回bucket的位置
                    return Ok((None, bucketpos));
                }
            }
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Cursor::new(Vec::with_capacity(bucketsize));
        // 写index
        buf.write_u64::<LittleEndian>(self.index)?;
        // 写is_overflow
        buf.write_u8(if self.is_overflow { 1 } else { 0 })?;
        // 写overflow_off
        if let Some(offset) = self.overflow_off {
            buf.write_u64::<LittleEndian>(offset)?;
        } else {
            buf.write_u64::<LittleEndian>(0)?;
        }
        // 写所有slot
        for slot in self.slots.iter() {
            buf.write_all(&slot.to_bytes()?)?;
        }
        // 不足则padding
        for _ in self.slots.len()..slotnums {
            buf.write_all(&vec![0;slotsize])?;
        }

        Ok(buf.into_inner())
    }
    fn write_bytes<W>(&self, writer: &mut W, offset: u64) -> Result<(), Error>
    where
        W: Write + Seek,
    {
        writer.seek(SeekFrom::Start(offset))?;
        let buf = self.to_bytes()?;
        writer.write_all(&buf)?;
        Ok(())
    }
}
