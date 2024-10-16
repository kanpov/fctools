use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{process::Child, task::JoinSet};

use crate::{
    fs_backend::FsBackend,
    process_spawner::ProcessSpawner,
    vmm::{
        arguments::{
            command_modifier::{apply_command_modifier_chain, CommandModifier},
            firecracker::{FirecrackerApiSocket, FirecrackerArguments, FirecrackerConfigurationOverride},
        },
        installation::VmmInstallation,
    },
};

use super::{create_file_with_tree, force_chown, join_on_set, VmmExecutor, VmmExecutorError};

/// An executor that uses the "firecracker" binary directly, without jailing it or ensuring it doesn't run as root.
/// This executor allows rootless execution, given that the user has access to /dev/kvm.
#[derive(Debug)]
pub struct UnrestrictedVmmExecutor {
    firecracker_arguments: FirecrackerArguments,
    command_modifier_chain: Vec<Box<dyn CommandModifier>>,
    remove_metrics_on_cleanup: bool,
    remove_logs_on_cleanup: bool,
    pipes_to_null: bool,
    id: Option<VmmId>,
}

impl UnrestrictedVmmExecutor {
    pub fn new(firecracker_arguments: FirecrackerArguments) -> Self {
        Self {
            firecracker_arguments,
            command_modifier_chain: Vec::new(),
            remove_metrics_on_cleanup: false,
            remove_logs_on_cleanup: false,
            pipes_to_null: false,
            id: None,
        }
    }

    pub fn command_modifier(mut self, command_modifier: impl CommandModifier + 'static) -> Self {
        self.command_modifier_chain.push(Box::new(command_modifier));
        self
    }

    pub fn command_modifiers(mut self, command_modifiers: impl IntoIterator<Item = Box<dyn CommandModifier>>) -> Self {
        self.command_modifier_chain.extend(command_modifiers);
        self
    }

    pub fn remove_metrics_on_cleanup(mut self) -> Self {
        self.remove_metrics_on_cleanup = true;
        self
    }

    pub fn remove_logs_on_cleanup(mut self) -> Self {
        self.remove_logs_on_cleanup = true;
        self
    }

    pub fn pipes_to_null(mut self) -> Self {
        self.pipes_to_null = true;
        self
    }

    pub fn id(mut self, id: VmmId) -> Self {
        self.id = Some(id);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VmmId(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VmmIdParseError {
    TooShort,
    TooLong,
    ContainsInvalidCharacter,
}

impl VmmId {
    pub fn new(id: impl Into<String>) -> Result<VmmId, VmmIdParseError> {
        let id = id.into();

        if id.len() < 5 {
            return Err(VmmIdParseError::TooShort);
        }

        if id.len() > 60 {
            return Err(VmmIdParseError::TooLong);
        }

        if id.chars().any(|c| !c.is_ascii_alphanumeric() && c != '-') {
            return Err(VmmIdParseError::ContainsInvalidCharacter);
        }

        Ok(Self(id))
    }
}

impl AsRef<str> for VmmId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<VmmId> for String {
    fn from(value: VmmId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use crate::vmm::executor::unrestricted::{VmmId, VmmIdParseError};

    #[test]
    fn vmm_id_rejects_when_too_short() {
        for l in 0..5 {
            let str = (0..l).map(|_| "l").collect::<String>();
            assert_eq!(VmmId::new(str), Err(VmmIdParseError::TooShort));
        }
    }

    #[test]
    fn vmm_id_rejects_when_too_long() {
        for l in 61..100 {
            let str = (0..l).map(|_| "L").collect::<String>();
            assert_eq!(VmmId::new(str), Err(VmmIdParseError::TooLong));
        }
    }

    #[test]
    fn vmm_id_rejects_when_invalid_character() {
        for c in ['~', '_', '$', '#', '+'] {
            let str = (0..10).map(|_| c).collect::<String>();
            assert_eq!(VmmId::new(str), Err(VmmIdParseError::ContainsInvalidCharacter));
        }
    }

    #[test]
    fn vmm_id_accepts_valid() {
        for str in ["vmm-id", "longer-id", "L1Nda74-", "very-loNg-ID"] {
            VmmId::new(str).unwrap();
        }
    }
}

impl VmmExecutor for UnrestrictedVmmExecutor {
    fn get_socket_path(&self, _installation: &VmmInstallation) -> Option<PathBuf> {
        match &self.firecracker_arguments.api_socket {
            FirecrackerApiSocket::Disabled => None,
            FirecrackerApiSocket::Enabled(path) => Some(path.clone()),
        }
    }

    fn inner_to_outer_path(&self, _installation: &VmmInstallation, inner_path: &Path) -> PathBuf {
        inner_path.to_owned()
    }

    fn traceless(&self) -> bool {
        false
    }

    async fn prepare(
        &self,
        _installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        fs_backend: Arc<impl FsBackend>,
        outer_paths: Vec<PathBuf>,
    ) -> Result<HashMap<PathBuf, PathBuf>, VmmExecutorError> {
        let mut join_set = JoinSet::new();

        for path in outer_paths.clone() {
            let fs_backend = fs_backend.clone();
            let process_spawner = process_spawner.clone();
            join_set.spawn(async move {
                if !fs_backend
                    .check_exists(&path)
                    .await
                    .map_err(VmmExecutorError::FsBackendError)?
                {
                    return Err(VmmExecutorError::ExpectedResourceMissing(path));
                }

                force_chown(&path, process_spawner.as_ref()).await
            });
        }

        if let FirecrackerApiSocket::Enabled(socket_path) = self.firecracker_arguments.api_socket.clone() {
            let fs_backend = fs_backend.clone();
            let process_spawner = process_spawner.clone();
            join_set.spawn(async move {
                if fs_backend
                    .check_exists(&socket_path)
                    .await
                    .map_err(VmmExecutorError::FsBackendError)?
                {
                    force_chown(&socket_path, process_spawner.as_ref()).await?;
                    fs_backend
                        .remove_file(&socket_path)
                        .await
                        .map_err(VmmExecutorError::FsBackendError)?;
                }

                Ok(())
            });
        }

        // Ensure argument paths exist
        if let Some(ref log_path) = self.firecracker_arguments.log_path {
            join_set.spawn(create_file_with_tree(fs_backend.clone(), log_path.clone()));
        }
        if let Some(ref metrics_path) = self.firecracker_arguments.metrics_path {
            join_set.spawn(create_file_with_tree(fs_backend.clone(), metrics_path.clone()));
        }

        join_on_set(join_set).await?;
        Ok(outer_paths.into_iter().map(|path| (path.clone(), path)).collect())
    }

    async fn invoke(
        &self,
        installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        configuration_override: FirecrackerConfigurationOverride,
    ) -> Result<Child, VmmExecutorError> {
        let mut arguments = self.firecracker_arguments.join(configuration_override);
        let mut binary_path = installation.firecracker_path.clone();
        apply_command_modifier_chain(&mut binary_path, &mut arguments, &self.command_modifier_chain);
        if let Some(ref id) = self.id {
            arguments.push("--id".to_string());
            arguments.push(id.as_ref().to_owned());
        }

        let child = process_spawner
            .spawn(&binary_path, arguments, self.pipes_to_null)
            .await
            .map_err(VmmExecutorError::ProcessSpawnFailed)?;
        Ok(child)
    }

    async fn cleanup(
        &self,
        _installation: &VmmInstallation,
        process_spawner: Arc<impl ProcessSpawner>,
        fs_backend: Arc<impl FsBackend>,
    ) -> Result<(), VmmExecutorError> {
        let mut join_set: JoinSet<Result<(), VmmExecutorError>> = JoinSet::new();

        if let FirecrackerApiSocket::Enabled(socket_path) = self.firecracker_arguments.api_socket.clone() {
            let process_spawner = process_spawner.clone();
            let fs_backend = fs_backend.clone();
            join_set.spawn(async move {
                if fs_backend
                    .check_exists(&socket_path)
                    .await
                    .map_err(VmmExecutorError::FsBackendError)?
                {
                    force_chown(&socket_path, process_spawner.as_ref()).await?;
                    fs_backend
                        .remove_file(&socket_path)
                        .await
                        .map_err(VmmExecutorError::FsBackendError)?;
                }
                Ok(())
            });
        }

        if self.remove_logs_on_cleanup {
            if let Some(ref log_path) = self.firecracker_arguments.log_path {
                let fs_backend = fs_backend.clone();
                let log_path = log_path.clone();
                join_set.spawn(async move {
                    fs_backend
                        .remove_file(&log_path)
                        .await
                        .map_err(VmmExecutorError::FsBackendError)
                });
            }
        }

        if self.remove_metrics_on_cleanup {
            if let Some(ref metrics_path) = self.firecracker_arguments.metrics_path {
                let fs_backend = fs_backend.clone();
                let metrics_path = metrics_path.clone();
                join_set.spawn(async move {
                    fs_backend
                        .remove_file(&metrics_path)
                        .await
                        .map_err(VmmExecutorError::FsBackendError)
                });
            }
        }

        join_on_set(join_set).await
    }
}
