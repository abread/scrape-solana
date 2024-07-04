use mmap_vec::MmapVecBuilder;

mod utils;
use utils::page_size;

#[test]
fn test_capacity() {
    macro_rules! simple_test_with_type {
        ($t:ident) => {
            let v = MmapVecBuilder::<$t>::new().try_build().unwrap();
            assert_eq!(v.capacity(), page_size() / std::mem::size_of::<$t>());
        };
    }

    simple_test_with_type!(u8);
    simple_test_with_type!(u64);
    simple_test_with_type!(i64);

    let v = MmapVecBuilder::<i64>::new()
        .capacity(page_size() / std::mem::size_of::<i64>() * 3 / 2)
        .try_build()
        .unwrap();
    assert_eq!(v.capacity(), 2 * page_size() / std::mem::size_of::<i64>());
}
