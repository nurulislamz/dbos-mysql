use super::Engine;
use crate::{Error, VersionRow};

impl Engine {
    pub fn application_version(&self) -> Option<&str> {
        self.config.application_version.as_deref()
    }

    pub async fn list_versions(&self) -> Result<Vec<VersionRow>, Error> {
        self.store.list_versions().await
    }

    pub async fn set_latest_version(&self, name: &str) -> Result<(), Error> {
        self.store.set_latest_version(name).await
    }
}
