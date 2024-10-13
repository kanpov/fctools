use std::{
    future::{Future, IntoFuture},
    path::Path,
    pin::Pin,
};

use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    task::JoinSet,
};

#[cfg(feature = "blocking-fs-backend")]
pub mod blocking;

/// An operation that has been scheduled by the FS backend. Such an operation must only be invoked once
/// IntoFuture is used or it is offloaded onto a JoinSet.
#[must_use = "FsOperations do nothing unless awaited or polled as a future, or offloaded onto a JoinSet"]
pub trait FsOperation<R: Send + 'static>:
    IntoFuture<
        Output = Result<R, std::io::Error>,
        IntoFuture = Pin<Box<dyn Future<Output = Result<R, std::io::Error>> + Send>>,
    > + Sized
{
    fn offload<E: From<std::io::Error> + Send + 'static>(self, join_set: &mut JoinSet<Result<R, E>>) {
        let future = self.into_future();
        join_set.spawn(async move { future.await.map_err(E::from) });
    }
}

pub trait FsFileHandle: AsyncRead + AsyncSeek + AsyncWrite + Send + Unpin {}

/// A filesystem backend provides fctools with filesystem operations on the host OS. The primary two viable
/// implementations of a filesystem backend on a modern Linux system are either blocking epoll wrapped in
/// Tokio's spawn_blocking, or asynchronous io-uring.
pub trait FsBackend: Send + Sync + 'static {
    fn check_exists(&self, path: &Path) -> impl FsOperation<bool>;

    fn remove_file(&self, path: &Path) -> impl FsOperation<()>;

    fn create_dir_all(&self, path: &Path) -> impl FsOperation<()>;

    fn create_file(&self, path: &Path) -> impl FsOperation<()>;

    fn write_all_to_file(&self, path: &Path, content: String) -> impl FsOperation<()>;

    fn rename_file(&self, source_path: &Path, destination_path: &Path) -> impl FsOperation<()>;

    fn open_file(&self, path: &Path) -> impl FsOperation<Pin<Box<dyn FsFileHandle>>>;

    fn remove_dir_all(&self, path: &Path) -> impl FsOperation<()>;

    fn copy(&self, source_path: &Path, destination_path: &Path) -> impl FsOperation<()>;

    fn hard_link(&self, source_path: &Path, destination_path: &Path) -> impl FsOperation<()>;
}
