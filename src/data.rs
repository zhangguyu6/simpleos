use std::borrow::Cow;
// .dat 文件中的记录结构
#[derive(Debug)]
struct Record<'a> {
    key: Cow<'a, [u8]>,
    value: Cow<'a, [u8]>,
}

impl<'a> Record<'a> {
    fn new<K, V>(key: K, val: V) -> Record<'a>
    where
        Cow<'a, [u8]>: From<K>,
        Cow<'a, [u8]>: From<V>,
    {
        Record {
            key: Cow::from(key),
            value: Cow::from(val),
        }
    }
}

