use std::{future::Future, path::Path};

use tokio::task::JoinSet;

#[cfg(feature = "blocking-io-backend")]
pub mod blocking;

pub trait FsOperation<R: Send + 'static> {
    fn offload<I>(self, join_set: &mut JoinSet<Result<R, std::io::Error>>);

    fn block_on(self) -> impl Future<Output = Result<R, std::io::Error>> + Send;
}

pub trait FsBackend: Send + Sync + 'static {
    fn check_exists(&self, path: &Path) -> impl FsOperation<bool>;

    fn remove_file(&self, path: &Path) -> impl FsOperation<()>;

    fn create_dir_all(&self, path: &Path) -> impl FsOperation<()>;

    fn create_file(&self, path: &Path) -> impl FsOperation<()>;
}
