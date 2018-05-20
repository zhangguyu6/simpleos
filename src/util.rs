use errors::Error;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;
pub type Timestamp = u64;

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
// 返回当前时间戳
pub fn get_timestamp() -> Result<Timestamp, Error> {
    let duration = SystemTime::now().elapsed()?;
    Ok(duration.as_secs() * 1000 + duration.subsec_millis() as u64)
}
#[inline]
pub fn roundup(size: usize, base: usize) -> usize {
    (size+base-1)/base
}
