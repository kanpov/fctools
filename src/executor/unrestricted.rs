use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::{process::Child, task::JoinSet};

use crate::{
    fs_backend::{FsBackend, FsOperation},
    shell_spawner::ShellSpawner,
};

use super::{
    arguments::{ConfigurationFileOverride, VmmApiSocket, VmmArguments},
    command_modifier::{apply_command_modifier_chain, CommandModifier},
    create_file_with_tree, force_chown,
    installation::VmmInstallation,
    join_on_set, VmmExecutor, VmmExecutorError,
};

/// An executor that uses the "firecracker" binary directly, without jailing it or ensuring it doesn't run as root.
/// This executor allows rootless execution, given that the user has access to /dev/kvm.
#[derive(Debug)]
pub struct UnrestrictedVmmExecutor {
    vmm_arguments: VmmArguments,
    command_modifier_chain: Vec<Box<dyn CommandModifier>>,
    remove_metrics_on_cleanup: bool,
    remove_logs_on_cleanup: bool,
    pipes_to_null: bool,
    id: Option<VmmId>,
}

impl UnrestrictedVmmExecutor {
    pub fn new(vmm_arguments: VmmArguments) -> Self {
        Self {
            vmm_arguments,
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
    use crate::executor::unrestricted::{VmmId, VmmIdParseError};

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

#[async_trait]
impl VmmExecutor for UnrestrictedVmmExecutor {
    fn get_socket_path(&self, _installation: &VmmInstallation) -> Option<PathBuf> {
        match &self.vmm_arguments.api_socket {
            VmmApiSocket::Disabled => None,
            VmmApiSocket::Enabled(path) => Some(path.clone()),
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
        shell_spawner: Arc<impl ShellSpawner>,
        fs_backend: Arc<impl FsBackend>,
        outer_paths: Vec<PathBuf>,
    ) -> Result<HashMap<PathBuf, PathBuf>, VmmExecutorError> {
        let mut join_set = JoinSet::new();

        for path in outer_paths.clone() {
            let fs_backend = fs_backend.clone();
            let shell_spawner = shell_spawner.clone();
            join_set.spawn(async move {
                if !fs_backend
                    .check_exists(&path)
                    .block_on()
                    .await
                    .map_err(VmmExecutorError::IoError)?
                {
                    return Err(VmmExecutorError::ExpectedResourceMissing(path.clone()));
                }

                force_chown(&path, shell_spawner.as_ref()).await?;
                Ok(())
            });
        }

        if let VmmApiSocket::Enabled(socket_path) = self.vmm_arguments.api_socket.clone() {
            let fs_backend = fs_backend.clone();
            let shell_spawner = shell_spawner.clone();
            join_set.spawn(async move {
                if fs_backend
                    .check_exists(&socket_path)
                    .block_on()
                    .await
                    .map_err(VmmExecutorError::IoError)?
                {
                    force_chown(&socket_path, shell_spawner.as_ref()).await?;
                    fs_backend
                        .remove_file(&socket_path)
                        .block_on()
                        .await
                        .map_err(VmmExecutorError::IoError)?;
                }

                Ok(())
            });
        }

        // Ensure argument paths exist
        if let Some(ref log_path) = self.vmm_arguments.log_path {
            join_set.spawn(create_file_with_tree(fs_backend.clone(), log_path.clone()));
        }
        if let Some(ref metrics_path) = self.vmm_arguments.metrics_path {
            join_set.spawn(create_file_with_tree(fs_backend.clone(), metrics_path.clone()));
        }

        join_on_set(join_set).await?;
        Ok(outer_paths.into_iter().map(|path| (path.clone(), path)).collect())
    }

    async fn invoke(
        &self,
        installation: &VmmInstallation,
        shell_spawner: Arc<impl ShellSpawner>,
        config_override: ConfigurationFileOverride,
    ) -> Result<Child, VmmExecutorError> {
        let arguments = self.vmm_arguments.join(config_override);
        let mut shell_command = format!("{} {arguments}", installation.firecracker_path.to_string_lossy());
        apply_command_modifier_chain(&mut shell_command, &self.command_modifier_chain);
        if let Some(ref id) = self.id {
            shell_command.push_str(" --id ");
            shell_command.push_str(id.as_ref());
        }

        let child = shell_spawner
            .spawn(shell_command, self.pipes_to_null)
            .await
            .map_err(VmmExecutorError::ShellSpawnFailed)?;
        Ok(child)
    }

    async fn cleanup(
        &self,
        _installation: &VmmInstallation,
        shell_spawner: Arc<impl ShellSpawner>,
        fs_backend: Arc<impl FsBackend>,
    ) -> Result<(), VmmExecutorError> {
        let mut join_set = JoinSet::new();

        if let VmmApiSocket::Enabled(socket_path) = self.vmm_arguments.api_socket.clone() {
            let shell_spawner = shell_spawner.clone();
            let fs_backend = fs_backend.clone();
            join_set.spawn(async move {
                if fs_backend
                    .check_exists(&socket_path)
                    .block_on()
                    .await
                    .map_err(VmmExecutorError::IoError)?
                {
                    force_chown(&socket_path, shell_spawner.as_ref()).await?;
                    fs_backend
                        .remove_file(&socket_path)
                        .block_on()
                        .await
                        .map_err(VmmExecutorError::IoError)?;
                }
                Ok(())
            });
        }

        if self.remove_logs_on_cleanup {
            if let Some(log_path) = self.vmm_arguments.log_path.clone() {
                let fs_backend = fs_backend.clone();
                join_set.spawn(async move {
                    fs_backend
                        .remove_file(&log_path)
                        .block_on()
                        .await
                        .map_err(VmmExecutorError::IoError)
                });
            }
        }

        if self.remove_metrics_on_cleanup {
            if let Some(metrics_path) = self.vmm_arguments.metrics_path.clone() {
                join_set.spawn(async move {
                    fs_backend
                        .remove_file(&metrics_path)
                        .block_on()
                        .await
                        .map_err(VmmExecutorError::IoError)
                });
            }
        }

        join_on_set(join_set).await
    }
}
