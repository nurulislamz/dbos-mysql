use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep_until, timeout_at};

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
    renewing: AbortHandle,
    watching: AbortHandle,
}

impl LeaseKeeper {
    pub fn spawn<F, Fut>(lease: Duration, max_run: Duration, mut renew: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        let (sender, lost) = watch::channel(None);
        let notify = sender.clone();
        let renewing = tokio::spawn(async move {
            let started = Instant::now();
            let usable = lease.mul_f64(5.0 / 6.0);
            let mut last_renewed = started;
            loop {
                // Renewals that fail fast must still report the loss before the lease expires.
                sleep_until((Instant::now() + lease / 3).min(last_renewed + usable)).await;
                if started.elapsed() >= max_run {
                    notify.send_replace(Some(LeaseLoss::MaxRunExceeded));
                    return;
                }
                if last_renewed.elapsed() >= usable {
                    notify.send_replace(Some(LeaseLoss::Unrenewable));
                    return;
                }
                let requested_at = Instant::now();
                match timeout_at(last_renewed + usable, renew()).await {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

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

    #[tokio::test]
    async fn a_panic_is_reported() {
        let keeper = LeaseKeeper::spawn(LEASE, LONG, || async { panic!("boom") });
        assert_eq!(lost(&keeper).await, LeaseLoss::Panicked);
    }
}
