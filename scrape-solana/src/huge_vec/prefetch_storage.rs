use std::{
    borrow::Borrow,
    collections::HashSet,
    fmt::Debug,
    io,
    ops::DerefMut,
    sync::{
        Arc, LazyLock, RwLock,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

pub static PREFETCH_THREADPOOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(0)
        .thread_name(|i| format!("prefetcher-{}", i))
        .build()
        .expect("failed to create prefetch threadpool")
});

use super::IndexedStorage;

pub(in crate::huge_vec) struct PrefetchableStore<T, InnerStore: IndexedStorage<T>> {
    store: Arc<RwLock<InnerStore>>,
    pending_reqs: HashSet<usize>,
    fetch_res_tx: SyncSender<(FetchReq, Result<T, InnerStore::Error>)>,
    fetch_res_rx: Receiver<(FetchReq, Result<T, InnerStore::Error>)>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::huge_vec) struct FetchReq {
    pub object_idx: usize,
}

impl<T, InnerStore: IndexedStorage<T>> PrefetchableStore<T, InnerStore>
where
    T: Debug + Send + 'static,
    InnerStore::Error: Send,
    InnerStore: Send + Sync + 'static,
{
    pub(in crate::huge_vec) fn new(chunk_store: InnerStore) -> io::Result<Self> {
        let store = Arc::new(RwLock::new(chunk_store));
        let max_pending_reqs = PREFETCH_THREADPOOL.current_num_threads() * 2;
        let (fetch_res_tx, fetch_res_rx) = sync_channel(max_pending_reqs);

        Ok(Self {
            store,
            pending_reqs: HashSet::with_capacity(max_pending_reqs),
            fetch_res_tx,
            fetch_res_rx,
        })
    }

    pub(in crate::huge_vec) fn invalidate_prefetches(&mut self) {
        // drop all pending prefetches
        while let Ok((req, _)) = self.fetch_res_rx.try_recv() {
            self.pending_reqs.remove(&req.object_idx);
        }
    }

    pub(in crate::huge_vec) fn store_no_invalidate(
        &mut self,
        object_idx: usize,
        object: impl Borrow<T>,
    ) -> Result<(), InnerStore::Error> {
        self.store
            .write()
            .expect("lock poisoned")
            .store(object_idx, object)
    }

    pub(in crate::huge_vec) fn truncate(&mut self, len: usize) -> Result<(), InnerStore::Error> {
        self.store.write().expect("lock poisoned").truncate(len)?;
        self.invalidate_prefetches();
        Ok(())
    }

    pub(in crate::huge_vec) fn clear(&mut self) -> Result<(), InnerStore::Error> {
        self.store.write().expect("lock poisoned").clear()?;
        self.invalidate_prefetches();
        Ok(())
    }

    pub(in crate::huge_vec) fn prefetch(&mut self, object_idx: usize) {
        if self.pending_reqs.len() < self.pending_reqs.capacity()
            && !self.pending_reqs.contains(&object_idx)
        {
            let store = Arc::clone(&self.store);
            let fetch_res_tx = self.fetch_res_tx.clone();
            PREFETCH_THREADPOOL.spawn_fifo(move || {
                let obj = store.read().expect("lock poisoned").load(object_idx);
                let _ = fetch_res_tx.send((FetchReq { object_idx }, obj));
            });

            self.pending_reqs.insert(object_idx);
        }
    }

    #[allow(clippy::type_complexity)]
    pub(in crate::huge_vec) fn load(
        &mut self,
        object_idx: usize,
    ) -> (Vec<(FetchReq, T)>, Result<T, InnerStore::Error>) {
        let mut prefetched_objects = Vec::new();

        let maybe_requested_object = if !self.pending_reqs.contains(&object_idx) {
            // load the requested object NOW
            self.store.read().expect("lock poisoned").load(object_idx)
        } else {
            // object is being prefetched, wait for it to arrive
            loop {
                match self.fetch_res_rx.recv() {
                    Ok((req, maybe_obj)) => {
                        // (pre)fetch done
                        self.pending_reqs.remove(&req.object_idx);

                        if req.object_idx == object_idx {
                            break maybe_obj;
                        } else if let Ok(obj) = maybe_obj {
                            prefetched_objects.push((req, obj));
                        }
                    }
                    Err(_) => {
                        unreachable!(
                            "fetch result channel closed but we have references to both ends? rust std bug"
                        );
                    }
                }
            }
        };

        prefetched_objects.extend(self.fetch_res_rx.try_iter().filter_map(|(req, maybe_obj)| {
            self.pending_reqs.remove(&req.object_idx);
            maybe_obj.ok().map(|obj| (req, obj))
        }));

        (prefetched_objects, maybe_requested_object)
    }

    pub(in crate::huge_vec) fn with_inner_mut<R>(
        &mut self,
        op: impl FnOnce(&mut InnerStore) -> R,
    ) -> R {
        let res = {
            let mut store = self.store.write().expect("lock poisoned");
            op(store.deref_mut())
        };

        self.invalidate_prefetches();
        res
    }
}
