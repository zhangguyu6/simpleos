use indexmap::IndexMap;
use indexmap::map::{Iter,IterMut};
use std::iter::{IntoIterator,FromIterator};

use std::hash::Hash;
use std::fmt;
use std::fmt::{Debug,Formatter};


// LRU缓存
// innercache : 保持插入顺序的的hashmap
// capacity : hashmap的容量
pub struct Cache<K, V> {
    innercache: IndexMap<K, V>,
    capacity: usize,
}

impl<K,V> Debug for Cache<K,V> 
where
    K: Hash + Eq + Clone + Debug,
    V: Debug {
    // add code here
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error>{
        let kvs:Vec<(&K,&V)> = self.innercache.iter().collect();
        write!(f, "{:?}",kvs)
    }
}


impl<K, V> Cache<K, V> {
    // 设定缓存最大容量
    pub fn with_capacity(n: usize) -> Self {
        if n == 0 {
            panic!("cann't allocate if n is zero");
        }
        Cache {
            innercache: IndexMap::with_capacity(n),
            capacity: n,
        }
    }
}
impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Clone,
{
    // FIXME: 只读
    pub fn lookup(&self,key:&K) -> Option<&V> {
        unimplemented!()
    }

    // 得到缓存中键的引用
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.innercache.swap_remove_full(key) {
            Some((_, k, v)) => {
                self.innercache.insert(k, v);
            }
            None => {}
        }
        self.innercache.get(key)
    }
    // 更新键,在键满时,移除index=0的键
    pub fn set(&mut self, key: K, val: V) {
        self.innercache.remove(&key);
        if (self.innercache.len()==self.capacity){
            self.innercache.swap_remove_index(0);
        }
        self.innercache.insert(key, val);
    }
    // 返回一个不可变迭代器
    pub fn iter(&self) -> Iter<K,V> {
        self.innercache.iter()
    }
    // 返回一个可变迭代器
    pub fn iter_mut(&mut self) -> IterMut<K,V> {
        self.innercache.iter_mut()
    }
}
// 不可变迭代器
// (&self.key, &self.value)
impl<'a,K,V> IntoIterator for &'a Cache<K,V> 
where K: Hash + Eq + Clone {
    type Item = (&'a K,&'a V);
    type IntoIter = Iter<'a,K,V>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
// 可变迭代器
// (&self.key, &mut self.value) 
impl<'a,K,V> IntoIterator for &'a mut  Cache<K,V> 
where K: Hash + Eq + Clone {
    type Item = (&'a K,&'a mut V);
    type IntoIter = IterMut<'a,K,V>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
// collect
impl<K,V> FromIterator<(K,V)> for Cache<K,V> 
where K: Hash + Eq + Clone{
    fn from_iter<I:IntoIterator<Item = (K,V)>>(iterable:I) -> Self {
        let map = IndexMap::from_iter(iterable);
        let capacity = map.capacity();
        Cache{
            innercache:map,
            capacity:capacity
        }
    }
}
// #[cfg(test)]
// mod tests {
//     #[test]
//     fn it_works() {
//         use indexmap::IndexMap;
//         let mut letters = IndexMap::new();
//         for ch in "a short treatise on fungi".chars() {
//             *letters.entry(ch).or_insert(0) += 1;
//         }

//         assert_eq!(letters[&'s'], 2);
//         assert_eq!(2 + 2, 4);
//     }
// }
