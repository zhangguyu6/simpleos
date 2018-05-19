use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// 返回key的u64哈希值
pub fn get_hash<T>(key: &T) -> u64
where
    T: AsRef<[u8]>,
{
    let mut hasher = DefaultHasher::new();
    let keyref: &[u8] = key.as_ref();
    keyref.hash(&mut hasher);
    hasher.finish()
}

// hash : u64 哈希值
// level: u64 当前bucket的最大数量为 (n+1)^2 (初始为0)
// split: u64 当前split的bucket数量 (初始为0)
// 根据hash level 和 split 计算 bucket的index
pub fn get_indexpos(hash: u64, level: u64, split: u64) -> u64 {
    let bucket = hash % (1 << level);
    if bucket < split {
        hash % (1 << (level + 1))
    } else {
        bucket
    }
}
