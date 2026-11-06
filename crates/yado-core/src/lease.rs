use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;
use tokio::time::{Instant, timeout_at};

use crate::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseLoss {
    TakenOver,
    Unrenewable,
    MaxRunExceeded,
    Panicked,
}

impl LeaseLoss {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TakenOver => "taken_over",
            Self::Unrenewable => "unrenewable",
            Self::MaxRunExceeded => "max_run_exceeded",
            Self::Panicked => "panicked",
        }
    }
}

#[derive(Debug)]
pub struct LeaseKeeper {
    lost: watch::Receiver<Option<LeaseLoss>>,
    poke: Arc<Notify>,
    renewing: AbortHandle,
    watching: AbortHandle,
}

impl LeaseKeeper {
    pub fn spawn<F, Fut>(lease: Duration, max_run: Duration, renew: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        Self::spawn_until(lease, max_run, renew, || std::future::ready(()))
    }

    /// `on_max_run` finishes before the loss is reported, so a caller that hears
    /// `MaxRunExceeded` can already claim the workflow.
    pub fn spawn_until<F, Fut, G, GFut>(
        lease: Duration,
        max_run: Duration,
        mut renew: F,
        mut on_max_run: G,
    ) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
        G: FnMut() -> GFut + Send + 'static,
        GFut: Future<Output = ()> + Send,
    {
        let (sender, lost) = watch::channel(None);
        let notify = sender.clone();
        let poke = Arc::new(Notify::new());
        let poked = poke.clone();
        let renewing = tokio::spawn(async move {
            let started = Instant::now();
            let max_deadline = started.checked_add(max_run);
            let usable = lease.mul_f64(5.0 / 6.0);
            let mut last_renewed = started;
            loop {
                let lease_deadline = last_renewed + usable;
                let deadline = max_deadline.map_or(lease_deadline, |max| lease_deadline.min(max));
                // Renewals that fail fast must still report the loss before the lease expires.
                let next = (Instant::now() + lease / 3).min(deadline);
                timeout_at(next, poked.notified()).await.ok();
                if started.elapsed() >= max_run {
                    on_max_run().await;
                    notify.send_replace(Some(LeaseLoss::MaxRunExceeded));
                    return;
                }
                if last_renewed.elapsed() >= usable {
                    notify.send_replace(Some(LeaseLoss::Unrenewable));
                    return;
                }
                let requested_at = Instant::now();
                match timeout_at(deadline, renew()).await {
                    Ok(Ok(())) => last_renewed = requested_at,
                    Ok(Err(Error::LeaseLost(_))) => {
                        notify.send_replace(Some(LeaseLoss::TakenOver));
                        return;
                    }
                    Ok(Err(error)) => log::warn!("could not renew a workflow lease: {error}"),
                    Err(_) => log::warn!("renewing a workflow lease timed out"),
                }
            }
        });
        let renewing_abort = renewing.abort_handle();
        let watching = tokio::spawn(async move {
            if let Err(error) = renewing.await
                && error.is_panic()
            {
                log::error!("the workflow lease task panicked");
                sender.send_replace(Some(LeaseLoss::Panicked));
            }
        })
        .abort_handle();
        Self {
            lost,
            poke,
            renewing: renewing_abort,
            watching,
        }
    }

    pub async fn lost(&self) -> LeaseLoss {
        let mut lost = self.lost.clone();
        let result = lost
            .wait_for(Option::is_some)
            .await
            .map(|value| value.unwrap_or(LeaseLoss::Panicked));
        match result {
            Ok(loss) => loss,
            Err(_) => std::future::pending().await,
        }
    }

    /// Runs the next renewal now. A renewal is what notices a cancel or a takeover, and it
    /// extends the lease only for its owner. A poke during a renewal runs one more after it.
    pub fn poke(&self) {
        self.poke.notify_one();
    }

    pub(crate) fn poker(&self) -> Arc<Notify> {
        self.poke.clone()
    }

    pub fn stop(&self) {
        self.renewing.abort();
        self.watching.abort();
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Why a step attempt ended before its body did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepExpiry {
    Deadline,
    MissedHeartbeat,
}

impl StepExpiry {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deadline => "deadline",
            Self::MissedHeartbeat => "missed_heartbeat",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum AttemptState {
    Running { last_beat: Instant },
    Finished,
    Expired(StepExpiry),
}

/// One step attempt's timers, apart from the owner lease. They need no stored clock,
/// because a crash restarts the attempt. Whichever of finish and expiry comes first stands.
#[derive(Debug)]
pub struct StepWatch {
    deadline: Option<Instant>,
    heartbeat: Option<Duration>,
    state: watch::Sender<AttemptState>,
}

impl StepWatch {
    pub fn start(timeout: Option<Duration>, heartbeat: Option<Duration>) -> Self {
        let now = Instant::now();
        Self {
            deadline: timeout.and_then(|timeout| now.checked_add(timeout)),
            heartbeat,
            state: watch::Sender::new(AttemptState::Running { last_beat: now }),
        }
    }

    /// Restarts the heartbeat timer. An attempt that has already expired stays expired.
    pub fn beat(&self) -> Result<(), StepExpiry> {
        self.settle(Some(AttemptState::Running {
            last_beat: Instant::now(),
        }))
    }

    /// The body has ended. `Err` means it ended too late, so its result must be dropped.
    pub fn finish(&self) -> Result<(), StepExpiry> {
        self.settle(Some(AttemptState::Finished))
    }

    /// Resolves when the attempt ends: why it expired, or `None` once it has finished in time.
    pub async fn expired(&self) -> Option<StepExpiry> {
        let mut state = self.state.subscribe();
        loop {
            let last_beat = match *state.borrow_and_update() {
                AttemptState::Running { last_beat } => last_beat,
                AttemptState::Finished => return None,
                AttemptState::Expired(why) => return Some(why),
            };
            let due = self
                .deadline
                .into_iter()
                .chain(self.heartbeat_due(last_beat))
                .min();
            // `changed` cannot fail, because `self` holds the sender.
            match due {
                Some(due) => {
                    if timeout_at(due, state.changed()).await.is_err() {
                        self.settle(None).ok();
                    }
                }
                None => state.changed().await.unwrap_or(()),
            }
        }
    }

    fn heartbeat_due(&self, last_beat: Instant) -> Option<Instant> {
        self.heartbeat
            .and_then(|heartbeat| last_beat.checked_add(heartbeat))
    }

    fn overdue(&self, last_beat: Instant) -> Option<StepExpiry> {
        let now = Instant::now();
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            return Some(StepExpiry::Deadline);
        }
        self.heartbeat_due(last_beat)
            .is_some_and(|due| now >= due)
            .then_some(StepExpiry::MissedHeartbeat)
    }

    fn settle(&self, in_time: Option<AttemptState>) -> Result<(), StepExpiry> {
        let mut outcome = Ok(());
        self.state.send_if_modified(|state| match *state {
            AttemptState::Finished => false,
            AttemptState::Expired(why) => {
                outcome = Err(why);
                false
            }
            AttemptState::Running { last_beat } => {
                if let Some(why) = self.overdue(last_beat) {
                    outcome = Err(why);
                    *state = AttemptState::Expired(why);
                    return true;
                }
                in_time.is_some_and(|next| {
                    *state = next;
                    true
                })
            }
        });
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use tokio::time::sleep;

    use super::*;

    const LEASE: Duration = Duration::from_millis(60);
    const LONG: Duration = Duration::from_secs(600);
    const WAIT: Duration = Duration::from_secs(5);

    async fn lost(keeper: &LeaseKeeper) -> LeaseLoss {
        tokio::time::timeout(WAIT, keeper.lost())
            .await
            .expect("the lease should be lost")
    }

    #[tokio::test]
    async fn renews_while_healthy() {
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let keeper = LeaseKeeper::spawn(LEASE, LONG, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        sleep(LEASE * 3).await;
        assert!(count.load(Ordering::SeqCst) >= 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), keeper.lost())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn stop_ends_renewal() {
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let keeper = LeaseKeeper::spawn(LEASE, LONG, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        keeper.stop();
        sleep(LEASE * 2).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn takeover_is_reported_at_once() {
        let keeper = LeaseKeeper::spawn(LEASE, LONG, || async {
            Err(Error::LeaseLost("a".to_owned()))
        });
        assert_eq!(lost(&keeper).await, LeaseLoss::TakenOver);
    }

    #[tokio::test]
    async fn repeated_database_errors_lose_the_lease() {
        let keeper = LeaseKeeper::spawn(LEASE, LONG, || async {
            Err(Error::Timeout(Duration::ZERO))
        });
        assert_eq!(lost(&keeper).await, LeaseLoss::Unrenewable);
    }

    #[tokio::test]
    async fn fast_failures_report_the_loss_before_the_lease_expires() {
        let lease = Duration::from_millis(1200);
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(lease, LONG, || async {
            Err(Error::Timeout(Duration::ZERO))
        });
        assert_eq!(lost(&keeper).await, LeaseLoss::Unrenewable);
        assert!(started.elapsed() < lease, "{:?}", started.elapsed());
    }

    #[tokio::test]
    async fn a_hung_renewal_loses_the_lease() {
        let keeper = LeaseKeeper::spawn(LEASE, LONG, std::future::pending);
        assert_eq!(lost(&keeper).await, LeaseLoss::Unrenewable);
    }

    #[tokio::test]
    async fn a_slow_ack_does_not_extend_the_renewed_lease() {
        let calls = Arc::new(AtomicU32::new(0));
        let second_request = Arc::new(tokio::sync::Notify::new());
        let entered = second_request.clone();
        let lease = Duration::from_millis(1200);
        let keeper = LeaseKeeper::spawn(lease, LONG, move || {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            let entered = entered.clone();
            async move {
                if first {
                    sleep(Duration::from_millis(450)).await;
                    Ok(())
                } else {
                    entered.notify_one();
                    std::future::pending().await
                }
            }
        });
        tokio::time::timeout(WAIT, second_request.notified())
            .await
            .expect("a second renewal should start");
        assert_eq!(
            tokio::time::timeout(lease / 3, keeper.lost())
                .await
                .expect("the lease lifetime starts before its acknowledgement"),
            LeaseLoss::Unrenewable
        );
    }

    #[tokio::test]
    async fn one_database_error_is_tolerated() {
        let count = Arc::new(AtomicU32::new(0));
        let keeper = LeaseKeeper::spawn(Duration::from_millis(300), LONG, move || {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    return Err(Error::Timeout(Duration::ZERO));
                }
                Ok(())
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(500), keeper.lost())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn max_run_time_ends_renewal() {
        let keeper = LeaseKeeper::spawn(LEASE, LEASE, || async { Ok(()) });
        assert_eq!(lost(&keeper).await, LeaseLoss::MaxRunExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn max_run_runs_its_hook_before_the_loss_is_visible() {
        let released = Arc::new(AtomicBool::new(false));
        let flag = released.clone();
        let keeper = LeaseKeeper::spawn_until(
            LEASE,
            LEASE / 6,
            || async { Ok(()) },
            move || {
                let flag = flag.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                }
            },
        );
        assert_eq!(lost(&keeper).await, LeaseLoss::MaxRunExceeded);
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn max_run_time_can_end_before_the_first_renewal() {
        let max_run = LEASE / 6;
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(LEASE, max_run, || async { Ok(()) });
        assert_eq!(lost(&keeper).await, LeaseLoss::MaxRunExceeded);
        assert!(started.elapsed() <= max_run);
    }

    #[tokio::test(start_paused = true)]
    async fn max_run_time_ends_a_hung_renewal() {
        let max_run = LEASE / 2;
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(LEASE, max_run, std::future::pending);
        assert_eq!(lost(&keeper).await, LeaseLoss::MaxRunExceeded);
        assert!(started.elapsed() <= max_run);
    }

    #[tokio::test(start_paused = true)]
    async fn max_run_beyond_the_clock_range_keeps_renewing() {
        let keeper = LeaseKeeper::spawn(LEASE, Duration::MAX, || async { Ok(()) });
        assert!(
            tokio::time::timeout(LEASE * 3, keeper.lost())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_panic_is_reported() {
        let keeper = LeaseKeeper::spawn(LEASE, LONG, || async { panic!("boom") });
        assert_eq!(lost(&keeper).await, LeaseLoss::Panicked);
    }

    #[tokio::test(start_paused = true)]
    async fn a_poke_renews_without_waiting_for_the_timer() {
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(LEASE, LONG, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        keeper.poke();
        while count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_poke_reports_a_cancel_at_once() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = cancelled.clone();
        let keeper = LeaseKeeper::spawn(LEASE, LONG, move || {
            let cancelled = flag.load(Ordering::SeqCst);
            async move {
                if cancelled {
                    return Err(Error::LeaseLost("a".to_owned()));
                }
                Ok(())
            }
        });
        tokio::time::sleep(LEASE / 2).await;
        cancelled.store(true, Ordering::SeqCst);
        let poked_at = Instant::now();
        keeper.poke();
        assert_eq!(lost(&keeper).await, LeaseLoss::TakenOver);
        assert_eq!(poked_at.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_poke_during_a_renewal_runs_another_after_it() {
        // That renewal may have started before the cancel committed, so it proves nothing.
        let calls = Arc::new(AtomicU32::new(0));
        let gate = Arc::new(Notify::new());
        let (counter, opened) = (calls.clone(), gate.clone());
        let keeper = LeaseKeeper::spawn(LEASE, LONG, move || {
            let call = counter.fetch_add(1, Ordering::SeqCst);
            let opened = opened.clone();
            async move {
                if call == 0 {
                    opened.notified().await;
                }
                Ok(())
            }
        });
        keeper.poke();
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        keeper.poke();
        let poked_at = Instant::now();
        gate.notify_one();
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(poked_at.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn pokes_do_not_keep_a_failing_lease_alive() {
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(LEASE, LONG, || async {
            Err(Error::Timeout(Duration::ZERO))
        });
        for _ in 0..10 {
            keeper.poke();
            tokio::time::sleep(LEASE / 20).await;
        }
        assert_eq!(lost(&keeper).await, LeaseLoss::Unrenewable);
        assert!(started.elapsed() < LEASE, "{:?}", started.elapsed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_poke_after_max_run_still_ends_the_run() {
        let keeper = LeaseKeeper::spawn(LEASE, LEASE / 6, || async { Ok(()) });
        tokio::time::sleep(LEASE / 3).await;
        keeper.poke();
        assert_eq!(lost(&keeper).await, LeaseLoss::MaxRunExceeded);
    }

    const LIMIT: Duration = Duration::from_secs(10);

    #[tokio::test(start_paused = true)]
    async fn a_deadline_ends_the_attempt_at_its_time() {
        let started = Instant::now();
        let watch = StepWatch::start(Some(LIMIT), None);
        assert_eq!(watch.expired().await, Some(StepExpiry::Deadline));
        assert_eq!(started.elapsed(), LIMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn silence_ends_the_attempt_one_heartbeat_after_the_last_beat() {
        let started = Instant::now();
        let watch = StepWatch::start(None, Some(LIMIT));
        sleep(LIMIT / 2).await;
        watch.beat().unwrap();
        assert_eq!(watch.expired().await, Some(StepExpiry::MissedHeartbeat));
        assert_eq!(started.elapsed(), LIMIT + LIMIT / 2);
    }

    #[tokio::test(start_paused = true)]
    async fn beats_keep_the_attempt_alive_past_many_heartbeats() {
        let watch = Arc::new(StepWatch::start(None, Some(LIMIT)));
        let waiting = tokio::spawn({
            let watch = watch.clone();
            async move { watch.expired().await }
        });
        for _ in 0..30 {
            sleep(LIMIT / 2).await;
            watch.beat().unwrap();
        }
        assert!(!waiting.is_finished());
        watch.finish().unwrap();
        assert_eq!(waiting.await.unwrap(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn beats_do_not_move_the_deadline() {
        let watch = StepWatch::start(Some(LIMIT), Some(LIMIT));
        sleep(LIMIT / 2).await;
        watch.beat().unwrap();
        sleep(LIMIT / 2).await;
        assert_eq!(watch.beat(), Err(StepExpiry::Deadline));
        assert_eq!(watch.expired().await, Some(StepExpiry::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_beat_does_not_revive_the_attempt() {
        let watch = StepWatch::start(None, Some(LIMIT));
        sleep(LIMIT * 2).await;
        assert_eq!(watch.beat(), Err(StepExpiry::MissedHeartbeat));
        assert_eq!(watch.beat(), Err(StepExpiry::MissedHeartbeat));
        assert_eq!(watch.expired().await, Some(StepExpiry::MissedHeartbeat));
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_finish_is_refused_even_if_nobody_watched_the_timer() {
        let watch = StepWatch::start(Some(LIMIT), None);
        sleep(LIMIT).await;
        assert_eq!(watch.finish(), Err(StepExpiry::Deadline));
        assert_eq!(watch.expired().await, Some(StepExpiry::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn a_finish_in_time_stands_after_the_timer_passes() {
        let watch = StepWatch::start(Some(LIMIT), Some(LIMIT));
        watch.finish().unwrap();
        sleep(LIMIT * 2).await;
        assert_eq!(watch.expired().await, None);
        assert_eq!(watch.finish(), Ok(()));
        assert_eq!(watch.beat(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_of_the_two_timers_decides() {
        let watch = StepWatch::start(Some(LIMIT * 2), Some(LIMIT));
        assert_eq!(watch.expired().await, Some(StepExpiry::MissedHeartbeat));
        let watch = StepWatch::start(Some(LIMIT), Some(LIMIT * 2));
        assert_eq!(watch.expired().await, Some(StepExpiry::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn no_timers_means_no_expiry() {
        let watch = StepWatch::start(None, None);
        assert!(tokio::time::timeout(LIMIT, watch.expired()).await.is_err());
        assert_eq!(watch.beat(), Ok(()));
    }
}
