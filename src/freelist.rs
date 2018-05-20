use errors::Error;

// 空闲区间列表
#[derive(Debug)]
pub struct FreeList {
    atags: Vec<Atag>,
    // 如果freelist溢出,overflow_off为续页位置
    overflow_off: Option<u64>,
    // freelist所能提供的最大区间
    maxfilesize: u32,
}

impl FreeList {
    // 初始空闲区间列表只包括一个空闲区间
    pub fn new(maxfilesize: u32) -> FreeList {
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

    // 所有已用空间,包括等待压缩的空间
    pub fn get_usedfilesize(&self) -> u32 {
        self.atags[self.atags.len() - 1].off
    }
    // 所有等待压缩的空间
    pub fn get_compfilesize(&self) -> u32 {
        let mut size = 0;
        for i in 0..self.atags.len() - 1 {
            size += self.atags[i].size;
        }
        size
    }
    // 最大文件大小
    pub fn get_maxfilesize(&self) -> u32 {
        self.maxfilesize
    }
    // 所有空闲空间
    pub fn get_freefilesize(&self) -> u32 {
        let mut size = 0;
        for atag in &self.atags {
            size += atag.size;
        }
        size
    }

    // 从freelist请求空间,返回空间的偏移
    pub fn request_room(&mut self, size: u32) -> Result<u32, Error> {
        match self.atags.iter().position(|ref atag| atag.size >= size) {
            None => Err(Error::Allocatefail("not enough free room".to_string())),
            Some(pos) => {
                let atag = &self.atags[pos].clone();
                let off = atag.off;
                if atag.size == size {
                    self.atags.remove(pos);
                } else {
                    let mut _atag = self.atags.get_mut(pos).unwrap(); 
                    *_atag =  atag.reduce(atag.size)?;
                }
                Ok(off)
            }
        }
    }
    // 将已释放的空间加入freelist
    pub fn free_room(&mut self, off: u32, size: u32) -> Result<(), Error> {
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
                    }
                    // 合并右侧
                    (None, Some(tag)) => {
                        self.atags[pos] = tag;
                    }
                    // 合并左侧
                    (Some(tag), None) => {
                        self.atags[pos - 1] = tag;
                    }
                    // 合并左右
                    (Some(_latag), Some(_)) => {
                        let atag = _latag.merge(&self.atags[pos]);
                        self.atags[pos - 1] = atag.unwrap();
                        self.atags.remove(pos);
                    }
                }
                Ok(())
            }
        }
    }
}

// 空闲区间
// off:  区间在.data文件中的开始位置
// size: 区间的长度
#[derive(Debug, Clone)]
struct Atag {
    off: u32,
    size: u32,
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
    fn reduce(&self, size: u32) -> Result<Atag, Error> {
        if size <= self.size {
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
