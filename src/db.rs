use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, RwLock};


#[derive(Debug)]
pub struct BucketDb{
    shared_bucket: Vec<Arc<Shared>>,
    capacity: usize,
}

impl BucketDb{
    pub fn new(capacity: usize) -> Self{
        let mut shared_bucket = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let shared = Arc::new(Shared::new());
            tokio::spawn(purge_expired_tasks(shared.clone()));
            shared_bucket.push(shared);
        }
        BucketDb{
            capacity,
            shared_bucket,
        }
    }

    fn hash(&self, key: Bytes) -> usize{
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes>{
        let index = self.hash(key.clone()) % self.capacity;
        let data = self.shared_bucket.get(index);
        match data {
            Some(data) => {
                let state = data.state.read().unwrap();
                state.entries.get(&key).map(|b| b.data.clone())
            },
            None => None
        }
    }


    pub fn keys(&self, key_start_op :Option<Bytes>) -> Vec<Bytes> {


        match key_start_op {
            Some(start_key) => {
                let key_start = start_key;
                let mut end = key_start.to_vec();
                if let Some(last_byte) = end.last_mut() {
                    *last_byte += 1;
                }
                let key_end = Bytes::from(end);
                self.shared_bucket.iter().flat_map(|bucket|{bucket.state.read().unwrap().entries.range(key_start.clone()..key_end.clone()).map(|(key, _)| key.clone()).collect::<Vec<_>>()}).collect()
            },
            None => {
                // self.shared_bucket.iter().flat_map(|bucket|{bucket.state.read().unwrap().entries.keys().collect()}).collect()
                self.shared_bucket.iter().flat_map(|bucket|{bucket.state.read().unwrap().entries.keys().map(|(key)| key.clone()).collect::<Vec<_>>()}).collect()
            }
        }

    }


    pub fn get_with_instant(&self, key: Bytes) -> Option<(Bytes, Option<Instant>)>{
        let index = self.hash(key.clone()) % self.capacity;
        let data = self.shared_bucket.get(index);
        match data {
            Some(data) => {
                let state = data.state.read().unwrap();
                if let Some(data) = state.entries.get(&key).map(|b| b.data.clone()){
                    let instant = state.key_expirations.get(&key).cloned();
                    Some((data.clone(), instant))
                }else {
                    None
                }

            },
            None => None
        }
    }


    pub fn set_newest(&self, key: Bytes, value: Bytes, expire: Instant){
        let index = self.hash(key.clone()) % self.capacity;
        let shared = self.shared_bucket.get(index).unwrap();
        let mut state = shared.state.write().unwrap();
        let mut notify = false;
        let expires_at = {
            let when = expire;
            notify = state.next_expiration().map(|expiration| expiration > when).unwrap_or(true);
            when
        };

        match state.key_expirations.get(&key) {
            Some(expire_time) => {
                if expire_time > &expires_at {
                    return
                }
            },
            None => {
            }
        }
        let prev = state.entries.insert(
            key.clone(),
            Entry{
                data: value,
                expires_at: Some(expires_at),
            }
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
                state.key_expirations.remove(&key);
            }
        }

        if let when = expires_at {
            state.expirations.insert((when.clone(), key.clone()));
            state.key_expirations.insert(key, when);
        }
        drop(state);
        if notify {
            shared.background_task.notify_one();
        }




    }

    pub fn set(&self, key: Bytes, value: Bytes, expire: Option<Duration>) {

        let index = self.hash(key.clone()) % self.capacity;
        let shared = self.shared_bucket.get(index).unwrap();
        let mut state = shared.state.write().unwrap();

        let mut notify = false;
        let expires_at = expire.map(|duration|{
            let when = Instant::now() + duration;

            notify = state.next_expiration().map(|expiration| expiration > when).unwrap_or(true);

            when
        });
        let prev = state.entries.insert(
            key.clone(),
            Entry{
                data: value,
                expires_at,
            }
        );
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
                state.key_expirations.remove(&key);

            }
        }

        if let Some(when) = expires_at {
            state.expirations.insert((when.clone(), key.clone()));
            state.key_expirations.insert(key.clone(), when);
        }
        drop(state);

        if notify {
            shared.background_task.notify_one();
        }
    }


    pub fn subscribe(& self, key: Bytes) -> broadcast::Receiver<Bytes>{

        let index = self.hash(key.clone()) % self.capacity;
        use std::collections::hash_map::Entry;
        let mut state;
        let shared = self.shared_bucket.get(index).unwrap();
        state = shared.state.write().unwrap();
        match state.pub_sub.entry(key) {
            Entry::Occupied(mut entry) => {entry.get_mut().subscribe()},
            Entry::Vacant(entry) => {
                let (tx, rx) = broadcast::channel(1024);
                entry.insert(tx);
                rx
            }
        }
    }

    pub fn publish(& self, key: Bytes, value: Bytes) -> usize{
        let index = self.hash(key.clone()) % self.capacity;
        let shared = self.shared_bucket.get(index).unwrap();
        let state = shared.state.write().unwrap();
        state.pub_sub.get(&key).map(|tx| tx.send(value).unwrap_or(0)).unwrap_or(0)
    }

    // fn shutdown_purge_task(&self){
    //     for Some(index) in self.shared_bucket.iter(){
    //
    //     }
    // }



}


#[derive(Debug)]
struct Shared{
    state: RwLock<State>,
    background_task: Notify,
}





#[derive(Debug)]
struct State {
    entries: BTreeMap<Bytes, Entry>,
    pub_sub: HashMap<Bytes, broadcast::Sender<Bytes>>,
    expirations: BTreeSet<(Instant, Bytes)>,
    key_expirations: HashMap<Bytes, Instant>,
    shutdown: bool,
}






#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}



impl Shared {

    fn new() -> Self {
        Shared{
            state: RwLock::new(State{
                entries: BTreeMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                key_expirations: HashMap::new(),
                shutdown: false,
            }),
            background_task: Default::default(),
        }
    }

    pub fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.write().unwrap();
        if state.shutdown{
            return None;
        }
        let state = &mut *state;
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now{
                return Some(when);
            }
            state.entries.remove(key);
            state.key_expirations.remove(key);
            state.expirations.remove(&(when, key.clone()));


        }
        None
    }
    fn is_shutdown(&self) -> bool {
        self.state.read().unwrap().shutdown
    }
}


impl State {

    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|expiration| expiration.0)
    }
}


async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {

            tokio::select! {
                _ = time::sleep_until(when) => {

                },
                _ = shared.background_task.notified() => {

                }
            }
        }else{
            shared.background_task.notified().await;
        }

    }
}


