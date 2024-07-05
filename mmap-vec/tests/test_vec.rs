use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use mmap_vec::{DefaultSegmentBuilder, MmapVec, MAX_EXP_GROWTH_BYTES};

mod data_gen;
pub use data_gen::*;

mod utils;
use utils::page_size;

macro_rules! assert_consistent_disksz {
    ($s:ident) => {{
        fn el_size<T>(_: &MmapVec<T>) -> usize {
            std::mem::size_of::<T>()
        }
        assert_eq!(
            $s.disk_size(),
            $s.capacity() * el_size(&$s),
            "disk size does not match capacity * element size"
        );
        assert!(
            $s.disk_size() % page_size() == 0
                || page_size() - ($s.disk_size() % page_size()) < el_size(&$s),
            "disk size wasting pages"
        );
    }};
}

#[test]
fn test_resize() {
    assert!(
        page_size() > 16 * std::mem::size_of_val(&ROW1),
        "page size too small for test(?)"
    );

    let mut v = MmapVec::<DataRow>::new();
    let mut expected_capacity = 0;
    assert_eq!(v.capacity(), expected_capacity);
    assert_consistent_disksz!(v);

    // Trigger first growth
    v.push(ROW1).unwrap();
    assert!(v.capacity() >= 1); // size of row is < page size
    assert_consistent_disksz!(v);
    expected_capacity = v.capacity();
    assert_eq!(v[0], ROW1);
    assert_eq!(&v[..], &[ROW1]);

    // Fill vec
    while v.len() < v.capacity() {
        v.push(ROW1).unwrap();
    }
    assert_eq!(v.capacity(), expected_capacity);
    assert_consistent_disksz!(v);

    // Exponential growth until MAX_EXP_GROWTH_BYTES
    while v.disk_size() < mmap_vec::MAX_EXP_GROWTH_BYTES {
        // Trigger second growth
        v.push(ROW2).unwrap();
        assert!(v.capacity() >= expected_capacity * 2);
        expected_capacity = v.capacity();
        assert_consistent_disksz!(v);

        // Fill vec
        while v.len() < v.capacity() {
            v.push(ROW1).unwrap();
        }
        assert_eq!(v.capacity(), expected_capacity);
        assert_consistent_disksz!(v);
    }

    // Trigger another growth
    v.push(ROW2).unwrap();

    // confirm it was not exponential
    assert!(v.capacity() - expected_capacity < MAX_EXP_GROWTH_BYTES);
    assert_consistent_disksz!(v);
}

#[test]
fn test_with_capacity() {
    let v = MmapVec::<DataRow>::with_capacity(500).unwrap();
    assert!(v.capacity() >= 500);
    assert_consistent_disksz!(v);
}

#[test]
fn test_assign() {
    let mut v = MmapVec::<DataRow>::new();

    v.push(ROW1).unwrap();
    assert_eq!(&v[..], &[ROW1]);

    v[0] = ROW2;
    assert_eq!(&v[..], &[ROW2]);
}

#[test]
fn test_pop() {
    let mut v = MmapVec::<DataRow>::new();
    v.push(ROW1).unwrap();
    v.push(ROW2).unwrap();
    assert_eq!(&v[..], &[ROW1, ROW2]);

    assert_eq!(v.pop(), Some(ROW2));
    assert_eq!(v.pop(), Some(ROW1));
    assert_eq!(v.pop(), None);
}

#[test]
fn test_drop() {
    let mut v = MmapVec::<DroppableRow>::new();
    let counter = Arc::new(AtomicU32::new(0));

    // Check push / pull inc
    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    v.pop();
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Check drop inc
    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    drop(v);
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
fn test_truncate() {
    let mut v = MmapVec::<DroppableRow>::new();
    let counter = Arc::new(AtomicU32::new(0));

    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(v.len(), 3);

    // Trigger with too high value
    v.truncate(500000);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(v.len(), 3);

    // Trigger resize
    v.truncate(2);
    assert_eq!(v.len(), 2);
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    v.truncate(0);
    assert_eq!(v.len(), 0);
    assert_eq!(counter.load(Ordering::Relaxed), 3);

    // Trigger on empty segment
    v.truncate(0);
    assert_eq!(v.len(), 0);
    assert_eq!(counter.load(Ordering::Relaxed), 3);
}

#[test]
fn test_truncate_first() {
    fn build_vec() -> MmapVec<u8> {
        let mut output = MmapVec::new();
        assert!(output.push(8).is_ok());
        assert!(output.push(5).is_ok());
        assert!(output.push(3).is_ok());
        assert!(output.push(12).is_ok());
        assert_eq!(&output[..], &[8, 5, 3, 12]);
        output
    }

    // Truncate 0
    {
        let mut v = build_vec();
        v.truncate_first(0);
        assert_eq!(&v[..], [8, 5, 3, 12]);
    }

    // Truncate half
    {
        let mut v = build_vec();
        v.truncate_first(2);
        assert_eq!(&v[..], [3, 12]);
    }

    // Truncate len
    {
        let mut v = build_vec();
        v.truncate_first(v.len());
        assert_eq!(&v[..], []);
    }

    // Truncate too much
    {
        let mut v = build_vec();
        v.truncate_first(v.len() + 1000);
        assert_eq!(&v[..], []);
    }
}

#[test]
fn test_clear() {
    let mut v = MmapVec::<DroppableRow>::new();
    let counter = Arc::new(AtomicU32::new(0));

    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert!(v.push(DroppableRow::new(counter.clone())).is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(v.len(), 2);

    // Trigger cleanup
    v.clear();
    assert_eq!(v.len(), 0);
    assert_eq!(counter.load(Ordering::Relaxed), 2);

    // Trigger on empty segment
    v.clear();
    assert_eq!(v.len(), 0);
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
fn test_equals() {
    let mut s1 = MmapVec::<i32>::new();
    let mut s2 = MmapVec::<i32>::new();

    // Check when empty.
    assert_eq!(s1, s2);

    // Check with different size.
    s1.push(42).unwrap();
    s1.push(17).unwrap();
    assert_ne!(s1, s2);

    // Check equals again but with data this time.
    s2.push(42).unwrap();
    s2.push(17).unwrap();
    assert_eq!(s1, s2);

    // Check different data.
    s1.push(15).unwrap();
    s2.push(-15).unwrap();
    assert_ne!(s1, s2);
}

#[test]
fn test_try_clone_null() {
    let mut s1 = MmapVec::<i32>::default();
    assert_eq!(s1.capacity(), 0);

    // Clone and check equals !
    let mut s2 = s1.try_clone().unwrap();
    assert_eq!(s2.capacity(), 0);
    assert_eq!(s1, s2);

    // Push data and check segment are different.
    s1.push(-8).unwrap();
    s2.push(93).unwrap();

    assert_eq!(&s1[..], [-8]);
    assert_eq!(&s2[..], [93]);
}

#[test]
fn test_try_clone_with_data() {
    let mut s1 = MmapVec::<i32>::new();
    s1.push(42).unwrap();
    s1.push(17).unwrap();

    // Clone and check equals !
    let mut s2 = s1.try_clone().unwrap();
    assert_eq!(s1, s2);
    assert_eq!(&s1[..], [42, 17]);

    // Push data and check segment are different.
    s1.push(-8).unwrap();
    s2.push(93).unwrap();

    assert_eq!(&s1[..], [42, 17, -8]);
    assert_eq!(&s2[..], [42, 17, 93]);
}

#[test]
fn test_advice_prefetch() {
    // Test prefetch with null
    {
        let v = MmapVec::<i32>::new();
        v.advice_prefetch_all_pages();
        v.advice_prefetch_page_at(0);
        v.advice_prefetch_page_at(42);
    }

    // Test prefetch wih no data
    {
        let v = MmapVec::<i32>::new();
        v.advice_prefetch_all_pages();
        v.advice_prefetch_page_at(0);
        v.advice_prefetch_page_at(18);
        v.advice_prefetch_page_at(25);
    }

    // Test prefetch with data
    {
        let mut v = MmapVec::<i32>::new();
        assert!(v.push(5).is_ok());
        assert!(v.push(9).is_ok());
        assert!(v.push(2).is_ok());
        assert!(v.push(8).is_ok());
        v.advice_prefetch_all_pages();
        v.advice_prefetch_page_at(0);
        v.advice_prefetch_page_at(18);
        v.advice_prefetch_page_at(25);
    }
}

#[test]
fn test_reserve_in_place() {
    // Test on null segment
    {
        let mut s = MmapVec::<i32>::new();
        assert_eq!(s.capacity(), 0);

        s.reserve(50).unwrap();
        assert!(s.capacity() >= 50);
        assert_consistent_disksz!(s);
    }

    // Test on valid segment with free space
    {
        let mut s = MmapVec::<i32>::with_capacity(100).unwrap();
        assert!(s.capacity() >= 100);

        let prev_capacity = s.capacity();
        assert!(s.reserve(50).is_ok());
        assert_eq!(s.capacity(), prev_capacity);
        assert_consistent_disksz!(s);
    }

    // Test on valid segment with free space
    {
        // Fill the vec
        let mut s = MmapVec::<i32>::with_capacity(100).unwrap();
        assert!(s.capacity() >= 100);
        assert_consistent_disksz!(s);

        // Reserve few bytes and check rounding
        while s.len() < s.capacity() {
            assert_eq!(s.push_within_capacity(0), Ok(()));
        }

        assert!(s.reserve(50).is_ok());
        assert!(s.capacity() >= 100 + 50);
        assert_consistent_disksz!(s);

        // Reserve one full page
        while s.len() < s.capacity() {
            assert_eq!(s.push_within_capacity(0), Ok(()));
        }

        assert!(s.reserve(1024).is_ok());
        assert!(s.capacity() >= 100 + 50 + 1024);
        assert_consistent_disksz!(s);

        // Reserve a single byte
        while s.len() < s.capacity() {
            assert_eq!(s.push_within_capacity(0), Ok(()));
        }

        assert!(s.reserve(1).is_ok());
        assert!(s.capacity() >= 100 + 50 + 1024 + 1);
        assert_consistent_disksz!(s);
    }
}

#[test]
fn test_reserve_in_place_drop() {
    let mut s = MmapVec::<DroppableRow>::with_capacity(100).unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let mut expected_capacity = s.capacity();
    assert!(expected_capacity >= 100);
    let mut expected_len = s.len();
    assert_eq!(expected_len, 0);

    // Fill vec
    while s.len() < s.capacity() {
        assert!(s
            .push_within_capacity(DroppableRow::new(counter.clone()))
            .is_ok());
    }
    assert_eq!(s.capacity(), expected_capacity);
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(s.len(), expected_capacity);
    expected_len = expected_capacity;

    // Trigger resize
    assert!(s.reserve(50).is_ok());
    assert!(s.capacity() >= expected_capacity + 50);
    expected_capacity = s.capacity();
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    // Fill vec again
    assert!(s
        .push_within_capacity(DroppableRow::new(counter.clone()))
        .is_ok());
    assert_eq!(s.capacity(), expected_capacity);
    assert_eq!(s.len(), expected_len + 1);
    expected_len += 1;
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    drop(s);
    assert_eq!(counter.load(Ordering::Relaxed) as usize, expected_len);
}

#[test]
fn test_drop_file() {
    // Create vec.
    let vec = MmapVec::<i32>::with_capacity(100).unwrap();
    let path = vec.path();
    assert!(path.exists());

    // Drop vec and check file as been removed.
    drop(vec);
    assert!(!path.exists());
}

#[test]
fn test_try_from_array() {
    let vec = MmapVec::<_, DefaultSegmentBuilder>::try_from([8, 6, 4, -48, 16]).unwrap();
    assert_eq!(&vec[..], [8, 6, 4, -48, 16]);
}

#[test]
fn test_try_from_slice() {
    let vec = MmapVec::<_, DefaultSegmentBuilder>::try_from([8, 6, 4, -48, 16].as_slice()).unwrap();
    assert_eq!(&vec[..], [8, 6, 4, -48, 16]);
}

#[test]
fn test_try_from_vec() {
    let vec = MmapVec::<_, DefaultSegmentBuilder>::try_from(Vec::from([8, 6, 4, -48, 16])).unwrap();
    assert_eq!(&vec[..], [8, 6, 4, -48, 16]);
}

#[test]
#[should_panic = "Zero sized type are not supported"]
fn test_zero_sized_type() {
    struct VoidStruct;

    let _vec = MmapVec::<VoidStruct>::with_capacity(50).unwrap();
}
