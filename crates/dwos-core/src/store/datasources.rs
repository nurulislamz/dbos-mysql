use super::Store;
use crate::Error;
use crate::workflow::Lease;

impl Store {
    /// Succeeds only while this lease still owns its pending workflow. An expired lease is revoked here.
    pub async fn check_owner(&self, lease: &Lease) -> Result<(), Error> {
        self.attempt(|| async {
            let (tx, _) = self.owned_transaction(lease).await?;
            let tx = self.live_transaction(tx, lease).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}
