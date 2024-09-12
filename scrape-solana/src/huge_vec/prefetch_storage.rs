use std::{
    borrow::Borrow,
    collections::HashSet,
    io,
    ops::DerefMut,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Mutex, RwLock,
    },
    thread::JoinHandle,
};

use itertools::{Either, Itertools};

use super::IndexedStorage;

pub(in crate::huge_vec) struct PrefetchableStore<T, InnerStore: IndexedStorage<T>> {
    store: Arc<RwLock<InnerStore>>,
    pending_reqs: HashSet<usize>,
    fetch_req_tx: SyncSender<FetchReq>,
    fetch_res_rx: Receiver<(FetchReq, Result<T, InnerStore::Error>)>,
    join_handles: Vec<JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::huge_vec) struct FetchReq {
    pub object_idx: usize,
    pub required: bool,
}

impl<T, InnerStore: IndexedStorage<T>> PrefetchableStore<T, InnerStore>
where
    T: Send + 'static,
    InnerStore::Error: Send,
    InnerStore: Send + Sync + 'static,
{
    pub(in crate::huge_vec) fn new(
        chunk_store: InnerStore,
        num_workers: usize,
    ) -> io::Result<Self> {
        let store = Arc::new(RwLock::new(chunk_store));
        let (fetch_req_tx, fetch_req_rx) = sync_channel::<FetchReq>(num_workers * 2);
        let (fetch_res_tx, fetch_res_rx) = sync_channel(num_workers * 2);

        let fetch_req_rx = Arc::new(Mutex::new(fetch_req_rx));
        let join_handles = (0..num_workers)
            .map(|i| {
                let store = Arc::clone(&store);
                let fetch_req_rx = Arc::clone(&fetch_req_rx);
                let fetch_res_tx = fetch_res_tx.clone();

                std::thread::Builder::new()
                    .name(format!("storfetcher-{i}"))
                    .spawn(move || {
                        loop {
                            let req = {
                                match fetch_req_rx
                                    .lock()
                                    .expect("req channel rx lock poisoned")
                                    .recv()
                                {
                                    Ok(req) => req,
                                    Err(_) => break,
                                }
                            };

                            let object = {
                                let store = store.read().expect("store lock poisoned");
                                store.load(req.object_idx)
                            };

                            match fetch_res_tx.send((req, object)) {
                                Ok(_) => (),
                                Err(_) if !req.required => break,
                                Err(_) => {
                                    // required requests must always be processed
                                    // caller panicked
                                    panic!("failed to send required fetch response");
                                }
                            }
                        }
                    })
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self {
            store,
            pending_reqs: HashSet::with_capacity(num_workers * 2),
            fetch_req_tx,
            fetch_res_rx,
            join_handles,
        })
    }

    pub(in crate::huge_vec) fn invalidate_prefetches(&mut self) {
        // drop all pending prefetches
        while let Ok((req, _)) = self.fetch_res_rx.try_recv() {
            self.pending_reqs.remove(&req.object_idx);
            assert!(!req.required, "{req:?} required but dropped");
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
        if !self.pending_reqs.contains(&object_idx)
            && self
                .fetch_req_tx
                .try_send(FetchReq {
                    object_idx,
                    required: false,
                })
                .is_ok()
        {
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
                        // channel was closed, this is not normal

                        // check if fetcher threads panicked
                        let finished_threads: Vec<_>;
                        (finished_threads, self.join_handles) =
                            std::mem::take(&mut self.join_handles)
                                .into_iter()
                                .partition_map(|h| {
                                    if h.is_finished() {
                                        Either::Left(h)
                                    } else {
                                        Either::Right(h)
                                    }
                                });
                        for handle in finished_threads {
                            handle.join().expect("fetcher thread panicked");
                        }

                        panic!("fetcher threads closed fetch result channel (without panicking)");
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

impl<T, InnerStore: IndexedStorage<T>> Drop for PrefetchableStore<T, InnerStore> {
    fn drop(&mut self) {
        // drop req channel to signal fetcher threads to exit
        let fetch_req_tx = std::mem::replace(&mut self.fetch_req_tx, sync_channel(1).0);
        std::mem::drop(fetch_req_tx);

        // handle fetcher panics
        for handle in self.join_handles.drain(..) {
            handle.join().expect("fetcher thread panicked");
        }
    }
}
