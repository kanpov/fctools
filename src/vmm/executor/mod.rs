use std::{
    collections::HashMap,
    future::Future,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, LazyLock},
};

#[cfg(feature = "jailed-vmm-executor")]
use jailed::JailRenamerError;
use nix::unistd::{Gid, Uid};
#[cfg(feature = "unrestricted-vmm-executor")]
use tokio::task::JoinSet;
use tokio::{process::Child, task::JoinError};

use crate::{
    fs_backend::{FsBackend, FsBackendError},
    process_spawner::ProcessSpawner,
};

use super::{arguments::VmmConfigurationOverride, installation::VmmInstallation};

#[cfg(feature = "jailed-vmm-executor")]
#[cfg_attr(docsrs, doc(cfg(feature = "jailed-vmm-executor")))]
pub mod jailed;
#[cfg(feature = "unrestricted-vmm-executor")]
#[cfg_attr(docsrs, doc(cfg(feature = "unrestricted-vmm-executor")))]
pub mod unrestricted;

pub(crate) static PROCESS_UID: LazyLock<Uid> = LazyLock::new(|| nix::unistd::geteuid());
pub(crate) static PROCESS_GID: LazyLock<Gid> = LazyLock::new(|| nix::unistd::getegid());

/// An error emitted by a [VmmExecutor].
#[derive(Debug, thiserror::Error)]
pub enum VmmExecutorError {
    #[error("A non-FS I/O error occurred: `{0}")]
    IoError(std::io::Error),
    #[error("An I/O error emitted by an FS backend occurred: `{0}`")]
    FsBackendError(FsBackendError),
    #[error("An ownership change as part of an ownership upgrade or downgrade failed: `{0}")]
    ChangeOwnerError(ChangeOwnerError),
    #[error("Joining on a spawned async task failed: `{0}`")]
    TaskJoinFailed(JoinError),
    #[error("Spawning an auxiliary or primary process via the process spawner failed: `{0}`")]
    ProcessSpawnFailed(std::io::Error),
    #[error("A passed-in resource at the path `{0}` was expected but doesn't exist or isn't accessible")]
    ExpectedResourceMissing(PathBuf),
    #[error("A directory that is supposed to have a parent in the filesystem has none")]
    ExpectedDirectoryParentMissing,
    #[cfg(feature = "jailed-vmm-executor")]
    #[cfg_attr(docsrs, doc(cfg(feature = "jailed-vmm-executor")))]
    #[error("Invoking the jail renamer to produce an inner path failed: `{0}`")]
    JailRenamerFailed(JailRenamerError),
    #[error("Another error occurred: `{0}`")]
    Other(Box<dyn std::error::Error + Send>),
}

/// An error that can occur when changing the owner to accommodate for ownership upgrades and/or downgrades.
#[derive(Debug, thiserror::Error)]
pub enum ChangeOwnerError {
    #[error("Spawning a \"chown\" process failed: `{0}`")]
    ProcessSpawnFailed(std::io::Error),
    #[error("Waiting on the completion of the \"chown\" process failed: `{0}`")]
    ProcessWaitFailed(std::io::Error),
    #[error("The \"chown\" process exited with a non-zero exit status: `{0}`")]
    ProcessExitedWithWrongStatus(ExitStatus),
    #[error("An in-process recursive chown implementation in the filesystem backend failed: `{0}`")]
    FsBackendError(FsBackendError),
}

/// A [VmmExecutor] manages the environment of a VMM, correctly invoking its process, while
/// setting up and subsequently cleaning its environment. This allows modularity between different modes of VMM execution.
pub trait VmmExecutor: Send + Sync {
    /// Get the host location of the VMM socket, if one exists.
    fn get_socket_path(&self, installation: &VmmInstallation) -> Option<PathBuf>;

    /// Resolves an inner path into an outer path.
    fn inner_to_outer_path(&self, installation: &VmmInstallation, inner_path: &Path) -> PathBuf;

    // Returns a boolean determining whether this executor leaves any traces on the host filesystem after cleanup.
    fn is_traceless(&self) -> bool;

    // Returns the UID and GID of the user to downgrade ownership to, if one was configured.
    fn get_ownership_downgrade(&self) -> Option<(Uid, Gid)>;

    /// Prepare all transient resources for the VMM invocation.
    fn prepare(
        &self,
        installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        fs_backend: Arc<impl FsBackend>,
        outer_paths: Vec<PathBuf>,
    ) -> impl Future<Output = Result<HashMap<PathBuf, PathBuf>, VmmExecutorError>> + Send;

    /// Invoke the VMM on the given [VmmInstallation] and return the spawned async [Child] process.
    fn invoke(
        &self,
        installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        configuration_override: VmmConfigurationOverride,
    ) -> impl Future<Output = Result<Child, VmmExecutorError>> + Send;

    /// Clean up all transient resources of the VMM invocation.
    fn cleanup(
        &self,
        installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        fs_backend: Arc<impl FsBackend>,
    ) -> impl Future<Output = Result<(), VmmExecutorError>> + Send;
}

pub(crate) async fn change_owner(
    path: &Path,
    uid: Uid,
    gid: Gid,
    upgrade: bool,
    process_spawner: &impl ProcessSpawner,
    fs_backend: &impl FsBackend,
) -> Result<(), ChangeOwnerError> {
    // use "chown" process spawning for upgrades since they require privilege acquiry that can't be done on the control process
    // for downgrades or other types of ownership changes, use an in-process async implementation from the FS backend
    if upgrade {
        let mut child = process_spawner
            .spawn(
                &PathBuf::from("chown"),
                vec![
                    "-f".to_string(),
                    "-R".to_string(),
                    format!("{uid}:{gid}"),
                    path.to_string_lossy().into_owned(),
                ],
                false,
            )
            .await
            .map_err(ChangeOwnerError::ProcessSpawnFailed)?;
        let exit_status = child.wait().await.map_err(ChangeOwnerError::ProcessWaitFailed)?;

        // code 256 means that a concurrent chown is being called and the chown will still be applied, so this error can
        // "safely" be ignored, which is better than inducing the overhead of global locking on chown paths.
        if !exit_status.success() && exit_status.into_raw() != 256 {
            return Err(ChangeOwnerError::ProcessExitedWithWrongStatus(exit_status));
        }
    } else {
        fs_backend
            .chownr(path, uid, gid)
            .await
            .map_err(ChangeOwnerError::FsBackendError)?;
    }

    Ok(())
}

async fn create_file_with_tree(
    fs_backend: Arc<impl FsBackend>,
    process_spawner: Arc<impl ProcessSpawner>,
    downgrade: Option<(Uid, Gid)>,
    path: PathBuf,
) -> Result<(), VmmExecutorError> {
    if let Some(parent_path) = path.parent() {
        if process_spawner.upgrades_ownership() {
            change_owner(
                &parent_path,
                *PROCESS_UID,
                *PROCESS_GID,
                true,
                process_spawner.as_ref(),
                fs_backend.as_ref(),
            )
            .await
            .map_err(VmmExecutorError::ChangeOwnerError)?;
        }

        fs_backend
            .create_dir_all(parent_path)
            .await
            .map_err(VmmExecutorError::FsBackendError)?;
    }

    fs_backend
        .create_file(&path)
        .await
        .map_err(VmmExecutorError::FsBackendError)?;

    if let Some((uid, gid)) = downgrade {
        change_owner(&path, uid, gid, false, process_spawner.as_ref(), fs_backend.as_ref())
            .await
            .map_err(VmmExecutorError::ChangeOwnerError)?;
    }

    Ok(())
}

#[cfg(feature = "unrestricted-vmm-executor")]
async fn join_on_set(mut join_set: JoinSet<Result<(), VmmExecutorError>>) -> Result<(), VmmExecutorError> {
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(result) => {
                if let Err(err) = result {
                    join_set.abort_all();
                    return Err(err);
                }
            }
            Err(err) => {
                join_set.abort_all();
                return Err(VmmExecutorError::TaskJoinFailed(err));
            }
        }
    }

    Ok(())
}
