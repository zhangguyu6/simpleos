use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use errors::Error;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

// Atag的大小
const atagsize: usize = 16;
// 一页Freelist的大小
// 127*16+16
const freelistsize: usize = 2048;
// 一页Freelsit中atag的大小
const atagnums: usize = 127;

// 空闲区间列表
#[derive(Debug)]
pub struct FreeList {
    atags: Vec<Atag>,
    // 如果freelist溢出,overflow_off为续页位置
    overflow_off: Option<u64>,
    // freelist所能提供的最大区间
    maxfilesize: u64,
}

impl FreeList {
    // 初始空闲区间列表只包括一个空闲区间
    pub fn new(maxfilesize: u64) -> FreeList {
        let starttag = Atag {
            off: 0,
            size: maxfilesize,
        };
        FreeList {
            atags: vec![starttag],
            overflow_off: None,
            maxfilesize: maxfilesize,
        }
    }
    fn read_from<R>(reader: &mut R, offset: u64) -> Result<FreeList, Error>
    where
        R: Read + Seek,
    {
        let mut atags = Vec::new();
        let mut _offset = offset;
        let mut overflow_off = None;
        loop {
            reader.seek(SeekFrom::Start(_offset))?;
            for i in 0..127 {
                let off = reader.read_u64::<LittleEndian>()?;
                let len = reader.read_u64::<LittleEndian>()?;
                if len == 0 {
                    break;
                } else {
                    atags.push(Atag {
                        off: off,
                        size: len,
                    })
                }
            }
            reader.seek(SeekFrom::Start(offset + (atagsize * 127) as u64))?;
            let overflow = reader.read_u64::<LittleEndian>()?;
            if overflow == 0 {
                break;
            } else {
                _offset = overflow;
                if overflow_off.is_none() {
                    overflow_off = Some(overflow);
                }
            };
        }
        let maxfilesize = reader.read_u64::<LittleEndian>()?;
        Ok(FreeList {
            atags: atags,
            overflow_off: overflow_off,
            maxfilesize: maxfilesize,
        })
    }
    pub fn split(&self) -> Vec<FreeList> {
        let mut atags = self.atags.clone();
        let mut freelists = Vec::new();
        while atags.len() > atagnums {
            let newatags = atags.split_off(atagnums);
            freelists.push(FreeList {
                atags: atags,
                overflow_off: None,
                maxfilesize: self.maxfilesize,
            });
            atags = newatags;
        }
        freelists
    }
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Cursor::new(Vec::with_capacity(freelistsize));
        for slot in &self.atags {
            buf.write_u64::<LittleEndian>(slot.off)?;
            buf.write_u64::<LittleEndian>(slot.size)?;
        }
        for _ in self.atags.len()..atagnums {
            buf.write_all(&vec![0; atagsize])?;
        }
        buf.write_u64::<LittleEndian>(if self.overflow_off.is_none() {
            0
        } else {
            self.overflow_off.unwrap()
        })?;
        buf.write_u64::<LittleEndian>(self.maxfilesize);
        Ok(buf.into_inner())
    }

    fn write_bytes<W>(&self, writer: &mut W, offset: u64) -> Result<(), Error>
    where
        W: Write + Seek,
    {
        let buf: Vec<u8> = self.to_bytes()?;
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(&buf)?;
        Ok(())
    }

    // 所有已用空间,包括等待压缩的空间
    pub fn get_usedfilesize(&self) -> u64 {
        self.atags[self.atags.len() - 1].off
    }
    // 所有代压缩的空间
    pub fn get_compfilesize(&self) -> u64 {
        let mut size = 0;
        for i in 0..self.atags.len() - 1 {
            size += self.atags[i].size;
        }
        size
    }
    pub fn get_maxfilesize(&self) -> u64 {
        self.maxfilesize
    }
    // 所有空闲空间
    pub fn get_freefilesize(&self) -> u64 {
        let mut size = 0;
        for atag in &self.atags {
            size += atag.size;
        }
        size
    }

    // 从freelist请求空间,返回空间的初始位置
    // TODO: 应当返回合适的Error
    pub fn request_room(&mut self, size: u64) -> Result<u64, Error> {
        match self.atags.iter().position(|ref atag| atag.size >= size) {
            // FIXME
            None => Err(Error::Allocatefail("not enough free room".to_string())),
            Some(pos) => {
                let mut atag = self.atags[pos].clone();
                if atag.size == size {
                    self.atags.remove(pos);
                    Ok(atag.off)
                } else {
                    atag = atag.reduce(atag.size)?;
                    Ok(atag.off)
                }
            }
        }
    }
    // 将已释放的空间加入freelist
    pub fn free_room(&mut self, off: u64, size: u64) -> Result<(), Error> {
        // 使用二分法寻找空间的插入位置
        match self.atags.binary_search_by(|atag| atag.off.cmp(&off)) {
            // 不能对空间进行重复释放
            Ok(..) => Err(Error::Allocatefail(
                "can not free same room again".to_string(),
            )),
            Err(pos) => {
                let atag = Atag {
                    off: off,
                    size: size,
                };
                // 尝试合并插入位置左右的atag
                // 插入位置为0时,左侧atag不存在
                // 否则左侧atag的index为pos-1
                let _latag = if pos == 0 {
                    None
                } else {
                    atag.merge(&self.atags[pos - 1])
                };
                // 插入位置为len时,右侧atag不存在
                // 否则右侧atag的index为pos
                let _ratag = if pos == self.atags.len() {
                    None
                } else {
                    atag.merge(&self.atags[pos])
                };
                match (_latag, _ratag) {
                    // 无需考虑合并
                    (None, None) => {
                        self.atags.insert(pos, atag);
                        Ok(())
                    }
                    // 合并右侧
                    (None, Some(tag)) => {
                        self.atags[pos] = tag;
                        Ok(())
                    }
                    // 合并左侧
                    (Some(tag), None) => {
                        self.atags[pos - 1] = tag;
                        Ok(())
                    }
                    // 合并左右
                    (Some(_latag), Some(_)) => {
                        let atag = _latag.merge(&self.atags[pos]);
                        self.atags[pos - 1] = atag.unwrap();
                        self.atags.remove(pos);
                        Ok(())
                    }
                }
            }
        }
    }
}

// 空闲区间
// off: u64 区间在.dat文件中的开始位置
// size:u64 区间的长度
#[derive(Debug, Clone)]
struct Atag {
    off: u64,
    size: u64,
}

impl Atag {
    // 合并两个空闲区间
    fn merge(&self, other: &Atag) -> Option<Atag> {
        if self.off + self.size == other.off {
            Some(Atag {
                off: self.off,
                size: self.size + other.size,
            })
        } else if other.off + other.size == self.off {
            Some(Atag {
                off: other.off,
                size: other.size + self.size,
            })
        } else {
            None
        }
    }
    // 从空闲区间中扣除一定的大小
    fn reduce(&self, size: u64) -> Result<Atag, Error> {
        if size < self.size {
            Ok(Atag {
                off: self.off + size,
                size: self.size - size,
            })
        } else {
            Err(Error::Allocatefail(
                "reduce room size bigger than self".to_string(),
            ))
        }
    }
}
