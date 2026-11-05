use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;

use super::Engine;

/// What a committed write on this engine can change for a waiter.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum WakeKey {
    /// A message for this workflow, its end, or its cancel.
    Workflow(String),
    Event(String, String),
    Queue(String),
}

type Waiters = HashMap<WakeKey, Vec<Arc<Notify>>>;

#[derive(Debug, Default)]
pub(super) struct WakeHub {
    waiters: Mutex<Waiters>,
    received: AtomicU64,
}

impl WakeHub {
    fn lock(&self) -> MutexGuard<'_, Waiters> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Call this before the first read. A wake after this is kept for the next `wait`, so a write
    /// landing between a read and its wait is not lost.
    pub(super) fn waiter(self: &Arc<Self>, keys: Vec<WakeKey>) -> Waiter {
        let notify = Arc::new(Notify::new());
        let mut waiters = self.lock();
        for key in &keys {
            waiters.entry(key.clone()).or_default().push(notify.clone());
        }
        drop(waiters);
        Waiter {
            hub: self.clone(),
            keys,
            notify,
        }
    }

    /// Call this only after the write has committed.
    pub(super) fn wake(&self, key: &WakeKey) {
        if let Some(found) = self.lock().get(key) {
            found.iter().for_each(|notify| notify.notify_one());
        }
    }

    pub(super) fn wake_all(&self) {
        self.lock()
            .values()
            .flatten()
            .for_each(|notify| notify.notify_one());
    }
}

#[derive(Debug)]
pub(super) struct Waiter {
    hub: Arc<WakeHub>,
    keys: Vec<WakeKey>,
    notify: Arc<Notify>,
}

impl Waiter {
    /// Ends on a wake, or after `limit` so a write from another process is still seen.
    pub(super) async fn wait(&self, limit: Duration) {
        if tokio::time::timeout(limit, self.notify.notified())
            .await
            .is_ok()
        {
            self.hub.received.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut waiters = self.hub.lock();
        for key in &self.keys {
            if let Some(found) = waiters.get_mut(key) {
                found.retain(|notify| !Arc::ptr_eq(notify, &self.notify));
                if found.is_empty() {
                    waiters.remove(key);
                }
            }
        }
    }
}

impl Engine {
    /// Waits ended by a wake rather than a timer. Lets a test prove the wake did the work.
    pub fn wakeups_received(&self) -> u64 {
        self.wakeups.received.load(Ordering::Relaxed)
    }

    /// Keys with a local waiter. It returns to zero when the waiters leave.
    pub fn waiting_keys(&self) -> usize {
        self.wakeups.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Far longer than any test, so only a wake can end these waits.
    const NO_TIMER: Duration = Duration::from_secs(3600);

    fn key(name: &str) -> WakeKey {
        WakeKey::Queue(name.to_owned())
    }

    async fn ended_by_a_wake(waiter: &Waiter) {
        tokio::time::timeout(Duration::from_secs(30), waiter.wait(NO_TIMER))
            .await
            .expect("the wake was lost");
    }

    #[tokio::test]
    async fn a_wake_before_the_wait_is_kept() {
        let hub = Arc::new(WakeHub::default());
        let waiter = hub.waiter(vec![key("a")]);
        hub.wake(&key("a"));
        ended_by_a_wake(&waiter).await;
        assert_eq!(hub.received.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_wake_is_lost_in_a_tight_loop() {
        let hub = Arc::new(WakeHub::default());
        let (ping, pong) = (hub.waiter(vec![key("ping")]), hub.waiter(vec![key("pong")]));
        let remote = hub.clone();
        let other = tokio::spawn(async move {
            for _ in 0..20_000 {
                ended_by_a_wake(&ping).await;
                remote.wake(&key("pong"));
            }
        });
        for _ in 0..20_000 {
            hub.wake(&key("ping"));
            ended_by_a_wake(&pong).await;
        }
        other.await.unwrap();
    }

    #[tokio::test]
    async fn every_waiter_on_a_key_is_woken_and_other_keys_are_not() {
        let hub = Arc::new(WakeHub::default());
        let (first, second) = (hub.waiter(vec![key("a")]), hub.waiter(vec![key("a")]));
        let other = hub.waiter(vec![key("b")]);
        hub.wake(&key("a"));
        ended_by_a_wake(&first).await;
        ended_by_a_wake(&second).await;
        other.wait(Duration::from_millis(1)).await;
        assert_eq!(hub.received.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn one_waiter_can_listen_on_several_keys() {
        let hub = Arc::new(WakeHub::default());
        let waiter = hub.waiter(vec![key("a"), key("b")]);
        hub.wake(&key("b"));
        ended_by_a_wake(&waiter).await;
    }

    #[tokio::test]
    async fn wake_all_reaches_every_key() {
        let hub = Arc::new(WakeHub::default());
        let (first, second) = (hub.waiter(vec![key("a")]), hub.waiter(vec![key("b")]));
        hub.wake_all();
        ended_by_a_wake(&first).await;
        ended_by_a_wake(&second).await;
    }

    #[test]
    fn a_key_is_removed_when_its_last_waiter_leaves() {
        let hub = Arc::new(WakeHub::default());
        let first = hub.waiter(vec![key("a"), key("b")]);
        let second = hub.waiter(vec![key("a")]);
        assert_eq!(hub.lock().len(), 2);
        drop(first);
        assert_eq!(hub.lock().len(), 1);
        drop(second);
        assert!(hub.lock().is_empty());
    }
}
