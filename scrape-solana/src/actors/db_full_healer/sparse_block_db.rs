use std::fs::{self, File};
use std::io::{self};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Barrier, Condvar, LazyLock, Mutex};

use eyre::eyre;

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;

use crate::db;
use crate::model::Block;

static IO_THREAD_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .thread_name(|i| format!("spdbio{i}"))
        .num_threads(0)
        .build()
        .unwrap()
});

// Note: see uses, affects storage format
const SLOT_SPREAD: u64 = 16;

pub struct SparseBlockDb {
    root: PathBuf,
    pending_write_count: Mutex<u64>,
    pending_write_zeroed: Condvar,
}

pub fn recover_blocks_from_db<T: Send>(
    old_db: db::Db,
    close_signal: Receiver<T>,
    recovered_blocks_db_path: PathBuf,
) -> eyre::Result<(Receiver<T>, SparseBlockDb)> {
    if recovered_blocks_db_path.is_dir() {
        return Ok((close_signal, SparseBlockDb::open(recovered_blocks_db_path)));
    }

    let close_signal = Mutex::new(close_signal);
    let recovered_blocks_db = Arc::new(SparseBlockDb::open(
        recovered_blocks_db_path.with_extension("wip"),
    ));
    let completion_barrier = Barrier::new(3);

    rayon::in_place_scope({
        let close_signal = &close_signal;
        let recovered_blocks_db = &recovered_blocks_db;
        let completion_barrier = &completion_barrier;

        move |s| {
            let (left, _, right) = old_db.split();

            for side in [left, right] {
                s.spawn(move |_| {
                    for block in side.blocks() {
                        if let Err(TryRecvError::Disconnected) =
                            close_signal.lock().unwrap().try_recv()
                        {
                            break; // not return, to avoid deadlock on completion barrier
                        }

                        if let Ok(b) = block {
                            recovered_blocks_db.add(b);
                        }
                    }

                    completion_barrier.wait();
                });
            }

            // wait for both workers to finish
            completion_barrier.wait();
        }
    });

    // remove mutex from close_signal, it should now remain accessible from the outside
    let close_signal = close_signal.into_inner().unwrap();

    // if the close signal was triggered, we likely didn't finish
    if let Err(TryRecvError::Disconnected) = close_signal.try_recv() {
        return Err(eyre!("heal cancelled"));
    }

    // wait for all pending writes to finish
    recovered_blocks_db.await_pending_writes();

    // drop recovered db in old path
    std::mem::drop(Arc::into_inner(recovered_blocks_db).expect("db is not ready to drop!!"));

    // make sure even directory updates are flushed to disk
    rustix::fs::sync();

    // move the sparse db to the final location
    std::fs::rename(
        recovered_blocks_db_path.with_extension("wip"),
        &recovered_blocks_db_path,
    )
    .expect("failed to move sparse db to final location");

    // really make sure the db is on disk in its final location
    rustix::fs::sync();

    // re-open block db in new location
    Ok((close_signal, SparseBlockDb::open(recovered_blocks_db_path)))
}

impl SparseBlockDb {
    pub(super) fn open(root: PathBuf) -> Self {
        for i in 0..SLOT_SPREAD {
            // Note: tied to SparseBlockDb::block_path
            let path = root.join(format!("slots-{:#04X}", i));
            if !path.exists() {
                fs::create_dir_all(&path).expect("Failed to create directory");
            }
        }

        Self {
            root,
            pending_write_count: Mutex::new(0),
            pending_write_zeroed: Condvar::new(),
        }
    }

    fn add(self: &Arc<Self>, block: Block) {
        let block_path = self.block_path(block.slot);
        if block_path.exists() {
            return; // Block already stored
        }

        *self.pending_write_count.lock().unwrap() += 1;

        let this = Arc::clone(self);
        IO_THREAD_POOL.spawn(move || {
            let mut temp_file = tempfile::NamedTempFile::new_in(block_path.parent().unwrap())
                .expect("Failed to create temp file");

            {
                let mut writer = zstd::stream::Encoder::new(&mut temp_file, 22)
                    .expect("failed to create zstd-compressing writer");
                bincode::serialize_into(&mut writer, &block)
                    .expect("Failed to serialize block to file");
                writer.finish().expect("failed to flush block writer");
            }
            temp_file
                .persist(block_path)
                .expect("failed to persist block");

            let mut pending_write_count = this.pending_write_count.lock().unwrap();
            *pending_write_count -= 1;

            if *pending_write_count == 0 {
                this.pending_write_zeroed.notify_all();
            }
        });
    }

    pub(super) fn get(&self, slot: u64) -> Option<Block> {
        let block_path = self.block_path(slot);
        let file = match File::open(block_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return None; // Block not found
            }
            Err(e) => {
                panic!("Failed to open block file: {e}");
            }
        };

        let decoder = zstd::stream::Decoder::new(file).expect("failed to create zstd decoder");
        let block: Block = bincode::deserialize_from(decoder).expect("block corrupted");

        assert_eq!(block.slot, slot);

        Some(block)
    }

    pub fn limits(&self) -> Option<(u64, u64, u64)> {
        let mut slot_numbers: Vec<u64> = self.slot_numbers();

        if slot_numbers.is_empty() {
            None
        } else {
            slot_numbers.par_sort_unstable();
            let first = slot_numbers[0];
            let med = slot_numbers[slot_numbers.len() / 2];
            let last = slot_numbers[slot_numbers.len() - 1];
            Some((first, med, last))
        }
    }

    pub fn assert_sharding_consistency(&self, n: u64, i: u64) {
        let slot_numbers: Vec<u64> = self.slot_numbers();

        for slot in slot_numbers {
            if slot % n != i {
                panic!(
                    "Shard ID mismatch: expected {i}, got {} on {:?}",
                    slot % n,
                    self.block_path(slot),
                );
            }
        }
    }

    fn block_path(&self, slot: u64) -> PathBuf {
        self.root
            .join(format!("slots-{:#04X}", slot % SLOT_SPREAD))
            .join(format!("{:#016X}", slot))
    }

    fn await_pending_writes(&self) {
        let mut pending_write_count = self.pending_write_count.lock().unwrap();
        while *pending_write_count > 0 {
            pending_write_count = self.pending_write_zeroed.wait(pending_write_count).unwrap();
        }
    }

    fn slot_numbers(&self) -> Vec<u64> {
        IO_THREAD_POOL.install(|| {
            (0..SLOT_SPREAD)
                .into_par_iter()
                .flat_map(|i| {
                    let path = self.root.join(format!("slots-{:#04X}", i));
                    fs::read_dir(path)
                        .unwrap_or_else(|_| panic!("Failed to read directory"))
                        .filter_map(|entry| {
                            let entry = entry.unwrap();
                            let file_name = entry.file_name();
                            let file_name_str = file_name.to_string_lossy();
                            u64::from_str_radix(&file_name_str, 16).ok()
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
    }
}

impl Drop for SparseBlockDb {
    fn drop(&mut self) {
        self.await_pending_writes();

        // call sync to ensure changes are truly persisted to disk
        rustix::fs::sync();
    }
}
