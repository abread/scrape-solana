use std::sync::Condvar;
use std::{collections::BTreeMap, sync::Mutex};

/// A manager for locking ranges of a vector.
pub(crate) struct LockManager {
    locks: Mutex<BTreeMap<LockRange, LockData>>,
    wait_unlock: Condvar,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) struct LockRange(u64, u64);

pub(crate) struct RdLockGuard<'mgr> {
    lock_manager: &'mgr LockManager,
    range: LockRange,
}

pub(crate) struct WrLock<'mgr> {
    lock_manager: &'mgr LockManager,
    range: LockRange,
}

impl LockManager {
    pub(crate) fn rd_lock(&self, range: LockRange) -> RdLockGuard<'_> {
        let mut locks = self.locks.lock().expect("lock poisoned");

        // block until no write locks exist for requested range
        loop {
            let mut has_write_lock = false;
            for (lock_range, lock_data) in
                locks.range(LockRange(0, range.0)..=LockRange(range.1, u64::MAX))
            {
                if lock_range.1 < range.0 {
                    continue;
                }

                if lock_data.write_locked() {
                    has_write_lock = true;
                    break;
                }
            }

            if !has_write_lock {
                break;
            } else {
                // wait for unlock
                // during the wait new locks may appear so we must recheck them
                locks = self.wait_unlock.wait(locks).expect("lock poisoned");
            }
        }

        locks.entry(range).or_default().count += 1;

        RdLockGuard {
            lock_manager: self,
            range,
        }
    }

    pub(crate) fn wr_lock(&self, range: LockRange) -> WrLock<'_> {
        let mut locks = self.locks.lock().expect("lock poisoned");

        // block until no locks exist for requested range
        loop {
            let mut has_lock = false;
            for (lock_range, lock_data) in
                locks.range(LockRange(0, range.0)..=LockRange(range.1, u64::MAX))
            {
                if lock_range.1 < range.0 {
                    continue;
                }

                has_lock = true;
                break;
            }

            if !has_lock {
                break;
            } else {
                // wait for unlock
                // during the wait new locks may appear so we must recheck them
                locks = self.wait_unlock.wait(locks).expect("lock poisoned");
            }
        }

        let prev = locks.insert(range, LockData { count: -1 });
        debug_assert!(prev.is_none());

        WrLock {
            lock_manager: self,
            range,
        }
    }
}

impl<'mgr> RdLockGuard<'mgr> {
    pub(crate) fn len(&self) -> u64 {
        self.range.len()
    }

    pub(crate) fn shrink(self, offset: u64) -> RdLockGuard<'mgr> {
        let (new_range, _) = self.range.split_at(offset);

        {
            let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");

            let existing_lock = locks.get_mut(&self.range).expect("lock mgr inconsistency");
            existing_lock.count -= 1;
            if existing_lock.count == 0 {
                locks.remove(&self.range);
            }

            locks.entry(new_range).or_default().count += 1;
        }

        RdLockGuard {
            lock_manager: self.lock_manager,
            range: new_range,
        }
    }

    pub(crate) fn split_at(self, offset: u64) -> (RdLockGuard<'mgr>, RdLockGuard<'mgr>) {
        let (r1, r2) = self.range.split_at(offset);

        {
            let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");

            let existing_lock = locks.get_mut(&self.range).expect("lock mgr inconsistency");
            existing_lock.count -= 1;
            if existing_lock.count == 0 {
                locks.remove(&self.range);
            }

            locks.entry(r1).or_default().count += 1;
            locks.entry(r2).or_default().count += 1;
        }

        (
            RdLockGuard {
                lock_manager: self.lock_manager,
                range: r1,
            },
            RdLockGuard {
                lock_manager: self.lock_manager,
                range: r2,
            },
        )
    }
}

impl<'mgr> WrLock<'mgr> {
    pub(crate) fn len(&self) -> u64 {
        self.range.len()
    }

    pub(crate) fn shrink(self, offset: u64) -> WrLock<'mgr> {
        let (new_range, _) = self.range.split_at(offset);

        {
            let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");

            let prev = locks.remove(&self.range).expect("lock mgr inconsistency");
            debug_assert!(prev.count == -1);

            let prev = locks.insert(new_range, LockData { count: -1 });
            debug_assert!(prev.is_none());
        }

        WrLock {
            lock_manager: self.lock_manager,
            range: new_range,
        }
    }

    pub(crate) fn split_at(self, offset: u64) -> (WrLock<'mgr>, WrLock<'mgr>) {
        let (r1, r2) = self.range.split_at(offset);

        {
            let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");

            let prev = locks.remove(&self.range).expect("lock mgr inconsistency");
            debug_assert!(prev.count == -1);

            let prev1 = locks.insert(r1, LockData { count: -1 });
            debug_assert!(prev1.is_none());
            let prev2 = locks.insert(r2, LockData { count: -1 });
            debug_assert!(prev2.is_none());
        }

        (
            WrLock {
                lock_manager: self.lock_manager,
                range: r1,
            },
            WrLock {
                lock_manager: self.lock_manager,
                range: r2,
            },
        )
    }
}

impl PartialOrd for LockRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for LockRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).then(self.1.cmp(&other.1))
    }
}
impl LockRange {
    fn len(&self) -> u64 {
        self.1 - self.0 + 1
    }
    fn split_at(&self, offset: u64) -> (LockRange, LockRange) {
        assert!(offset + 1 < self.len(), "invalid offset");
        (
            LockRange(self.0, self.0 + offset + 1),
            LockRange(self.0 + offset + 2, self.1),
        )
    }
}

#[derive(PartialEq, Default)]
struct LockData {
    count: i64,
}
impl LockData {
    fn read_locked(&self) -> bool {
        self.count > 0
    }
    fn write_locked(&self) -> bool {
        self.count < 0
    }
}

impl Drop for RdLockGuard<'_> {
    fn drop(&mut self) {
        let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");
        let lock_data = locks
            .get_mut(&self.range)
            .expect("lock manager inconsistent");
        debug_assert!(lock_data.count > 0, "lock manager inconsistent");
        lock_data.count -= 1;

        if lock_data.count == 0 {
            locks.remove(&self.range);
        }
    }
}

impl Drop for WrLock<'_> {
    fn drop(&mut self) {
        let mut locks = self.lock_manager.locks.lock().expect("lock poisoned");
        debug_assert!(
            locks.get(&self.range) == Some(&LockData { count: -1 }),
            "lock manager inconsistent"
        );
        locks.remove(&self.range);
    }
}
