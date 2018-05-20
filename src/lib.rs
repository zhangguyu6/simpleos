#![feature(rustc_private)]
#![feature(duration_extras)]



extern crate indexmap;
extern crate byteorder;

mod data;
mod util;
mod freelist;
mod filepool;
mod index;
mod cache;
mod errors;


#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use std::hash::Hasher;
    #[test]
    fn it_works() {
        use std::collections::hash_map::DefaultHasher;
        let mut h = DefaultHasher::new();
        &[11,2].hash(&mut h);
        println!("{:?}",h.finish());
        assert_eq!(2 + 2, 4);
    }
}
 