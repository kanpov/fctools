use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Arc,
    time::Duration,
};

use crate::{
    executor::{arguments::FirecrackerConfigOverride, installation::FirecrackerInstallation, VmmExecutor},
    process::{HyperResponseExt, VmmProcess, VmmProcessError, VmmProcessPipes, VmmProcessState},
    shell_spawner::ShellSpawner,
};
use bytes::Bytes;
use configuration::{NewVmConfigurationApplier, VmConfiguration};
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, Request};
use models::{
    VmAction, VmActionType, VmApiError, VmBalloon, VmBalloonStatistics, VmCreateSnapshot, VmEffectiveConfiguration,
    VmFirecrackerVersion, VmInfo, VmMachineConfiguration, VmStateForUpdate, VmUpdateBalloon, VmUpdateBalloonStatistics,
    VmUpdateDrive, VmUpdateNetworkInterface, VmUpdateState,
};
use paths::{VmSnapshotPaths, VmStandardPaths};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{fs, io::AsyncWriteExt, process::ChildStdin};

pub mod configuration;
pub mod models;
pub mod paths;

#[derive(Debug)]
pub struct Vm<E: VmmExecutor, S: ShellSpawner> {
    vmm_process: VmmProcess<E, S>,
    is_paused: bool,
    configuration: Option<VmConfiguration>,
    standard_paths: VmStandardPaths,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmState {
    NotStarted,
    Running,
    Paused,
    Exited,
    Crashed(ExitStatus),
}

impl std::fmt::Display for VmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmState::NotStarted => write!(f, "Not started"),
            VmState::Running => write!(f, "Running"),
            VmState::Paused => write!(f, "Paused"),
            VmState::Exited => write!(f, "Exited"),
            VmState::Crashed(exit_status) => write!(f, "Crashed with exit status: {exit_status}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VmError {
    #[error("The underlying VMM process returned an error: `{0}`")]
    ProcessError(VmmProcessError),
    #[error("Expected the VM to be in a certain state, but it was actually in the `{actual}` state")]
    ExpectedState { expected: Vec<VmState>, actual: VmState },
    #[error("Expected the VM to have exited or crashed, but it was actually in the `{actual}` state")]
    ExpectedExitedOrCrashed { actual: VmState },
    #[error("Serde serialization or deserialization failed: `{0}`")]
    SerdeError(serde_json::Error),
    #[error("An async I/O operation failed: `{0}`")]
    IoError(tokio::io::Error),
    #[error(
        "The API socket returned an unsuccessful HTTP response with the `{status_code}` status code: `{fault_message}`"
    )]
    ApiRespondedWithFault {
        status_code: StatusCode,
        fault_message: String,
    },
    #[error(
        "The API HTTP request could not be constructed, likely due to an incorrect URI or HTTP configuration: `{0}`"
    )]
    ApiRequestNotConstructed(http::Error),
    #[error("The API HTTP response could not be received from the API socket: `{0}`")]
    ApiResponseCouldNotBeReceived(hyper::Error),
    #[error("Expected the API response to be empty, but it contained the following response body: `{0}`")]
    ApiResponseExpectedEmpty(String),
    #[error("No shutdown methods were specified for the VM shutdown operation")]
    NoShutdownMethodsSpecified,
    #[error("A future timed out according to the given timeout duration")]
    Timeout,
    #[error("Attempted to use a VM configuration with a disabled API socket, which is not supported")]
    DisabledApiSocketIsUnsupported,
    #[error("A path mapping was expected to be constructed by the executor, but was not returned")]
    MissingPathMapping,
}

#[derive(Debug)]
pub enum VmShutdownMethod {
    CtrlAltDel,
    PauseThenKill,
    WriteRebootToStdin(ChildStdin),
    Kill,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash)]
pub struct VmCleanupOptions {
    remove_logs: bool,
    remove_metrics: bool,
    remove_vsock: bool,
}

impl VmCleanupOptions {
    pub fn new() -> Self {
        Self {
            remove_logs: false,
            remove_metrics: false,
            remove_vsock: false,
        }
    }

    pub fn remove_all(mut self) -> Self {
        self.remove_logs = true;
        self.remove_metrics = true;
        self.remove_vsock = true;
        self
    }

    pub fn remove_logs(mut self) -> Self {
        self.remove_logs = true;
        self
    }

    pub fn remove_metrics(mut self) -> Self {
        self.remove_metrics = true;
        self
    }

    pub fn remove_vsock(mut self) -> Self {
        self.remove_vsock = true;
        self
    }
}

impl<E: VmmExecutor, S: ShellSpawner> Vm<E, S> {
    pub async fn prepare(
        executor: E,
        shell_spawner: S,
        installation: FirecrackerInstallation,
        configuration: VmConfiguration,
    ) -> Result<Self, VmError> {
        Self::prepare_arced(executor, Arc::new(shell_spawner), Arc::new(installation), configuration).await
    }

    pub async fn prepare_arced(
        executor: E,
        shell_spawner_arc: Arc<S>,
        installation_arc: Arc<FirecrackerInstallation>,
        mut configuration: VmConfiguration,
    ) -> Result<Self, VmError> {
        if executor.get_socket_path().is_none() {
            return Err(VmError::DisabledApiSocketIsUnsupported);
        }

        // compute outer paths from configuration
        let mut outer_paths = Vec::new();
        match configuration {
            VmConfiguration::New(ref config) => {
                outer_paths.push(config.boot_source.kernel_image_path.clone());

                if let Some(ref path) = config.boot_source.initrd_path {
                    outer_paths.push(path.clone());
                }

                for drive in &config.drives {
                    if let Some(ref path) = drive.path_on_host {
                        outer_paths.push(path.clone());
                    }
                }
            }
            VmConfiguration::FromSnapshot(ref config) => {
                outer_paths.push(config.load_snapshot.snapshot_path.clone());
                outer_paths.push(config.load_snapshot.mem_backend.backend_path.clone());
            }
        }

        // prepare
        let mut vm_process = VmmProcess::new_arced(executor, shell_spawner_arc, installation_arc, outer_paths);
        let mut path_mappings = vm_process.prepare().await.map_err(VmError::ProcessError)?;

        // set inner paths for configuration (to conform to FC expectations) based on returned mappings
        match configuration {
            VmConfiguration::New(ref mut config) => {
                config.boot_source.kernel_image_path = path_mappings
                    .remove(&config.boot_source.kernel_image_path)
                    .ok_or(VmError::MissingPathMapping)?;

                if let Some(ref mut path) = config.boot_source.initrd_path {
                    config.boot_source.initrd_path =
                        Some(path_mappings.remove(path).ok_or(VmError::MissingPathMapping)?);
                }

                for drive in &mut config.drives {
                    if let Some(ref mut path) = drive.path_on_host {
                        *path = path_mappings.remove(path).ok_or(VmError::MissingPathMapping)?;
                    }
                }
            }
            VmConfiguration::FromSnapshot(ref mut config) => {
                config.load_snapshot.snapshot_path = path_mappings
                    .remove(&config.load_snapshot.snapshot_path)
                    .ok_or(VmError::MissingPathMapping)?;
                config.load_snapshot.mem_backend.backend_path = path_mappings
                    .remove(&config.load_snapshot.mem_backend.backend_path)
                    .ok_or(VmError::MissingPathMapping)?;
            }
        };

        // generate accessible paths, ensure paths exist
        let mut accessible_paths = VmStandardPaths {
            drive_sockets: HashMap::new(),
            metrics_path: None,
            log_path: None,
            vsock_multiplexer_path: None,
            vsock_listener_paths: Vec::new(),
        };
        match configuration {
            VmConfiguration::New(ref config) => {
                for drive in &config.drives {
                    if let Some(ref socket) = drive.socket {
                        accessible_paths
                            .drive_sockets
                            .insert(drive.drive_id.clone(), vm_process.inner_to_outer_path(&socket));
                    }
                }

                if let Some(ref logger) = config.logger {
                    if let Some(ref log_path) = logger.log_path {
                        let new_log_path = vm_process.inner_to_outer_path(log_path);
                        prepare_file(&new_log_path, false).await?;
                        accessible_paths.log_path = Some(new_log_path);
                    }
                }

                if let Some(ref metrics) = config.metrics {
                    let new_metrics_path = vm_process.inner_to_outer_path(&metrics.metrics_path);
                    prepare_file(&new_metrics_path, false).await?;
                    accessible_paths.metrics_path = Some(new_metrics_path);
                }

                if let Some(ref vsock) = config.vsock {
                    let new_uds_path = vm_process.inner_to_outer_path(&vsock.uds_path);
                    prepare_file(&new_uds_path, true).await?;
                    accessible_paths.vsock_multiplexer_path = Some(new_uds_path);
                }
            }
            VmConfiguration::FromSnapshot(ref config) => {
                if let Some(ref logger) = config.logger {
                    if let Some(ref log_path) = logger.log_path {
                        let new_log_path = vm_process.inner_to_outer_path(log_path);
                        prepare_file(&new_log_path, false).await?;
                        accessible_paths.log_path = Some(new_log_path);
                    }
                }

                if let Some(ref metrics) = config.metrics {
                    let new_metrics_path = vm_process.inner_to_outer_path(&metrics.metrics_path);
                    prepare_file(&new_metrics_path, false).await?;
                    accessible_paths.metrics_path = Some(new_metrics_path);
                }
            }
        };

        Ok(Self {
            vmm_process: vm_process,
            is_paused: false,
            configuration: Some(configuration),
            standard_paths: accessible_paths,
        })
    }

    pub fn state(&mut self) -> VmState {
        match self.vmm_process.state() {
            VmmProcessState::Started => match self.is_paused {
                true => VmState::Paused,
                false => VmState::Running,
            },
            VmmProcessState::Exited => VmState::Exited,
            VmmProcessState::Crashed(exit_status) => VmState::Crashed(exit_status),
            _ => VmState::NotStarted,
        }
    }

    pub async fn start(&mut self, socket_wait_timeout: Duration) -> Result<(), VmError> {
        self.ensure_state(VmState::NotStarted)?;
        let configuration = self
            .configuration
            .take()
            .expect("No configuration cannot exist for a VM, unreachable");
        let socket_path = self
            .vmm_process
            .get_socket_path()
            .ok_or(VmError::DisabledApiSocketIsUnsupported)?;

        let mut config_override = FirecrackerConfigOverride::NoOverride;
        let mut no_api_calls = false;
        if let VmConfiguration::New(ref config) = configuration {
            if let NewVmConfigurationApplier::ViaJsonConfiguration(inner_path) = config.get_applier() {
                config_override = FirecrackerConfigOverride::Enable(inner_path.clone());
                prepare_file(inner_path, true).await?;
                fs::write(
                    self.vmm_process.inner_to_outer_path(inner_path),
                    serde_json::to_string(config).map_err(VmError::SerdeError)?,
                )
                .await
                .map_err(VmError::IoError)?;
                no_api_calls = true;
            }
        }

        self.vmm_process
            .invoke(config_override)
            .await
            .map_err(VmError::ProcessError)?;

        tokio::time::timeout(socket_wait_timeout, async move {
            loop {
                if fs::try_exists(&socket_path).await? {
                    break;
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| VmError::Timeout)?
        .map_err(VmError::IoError)?;

        match &configuration {
            VmConfiguration::New(config) => {
                if no_api_calls {
                    return Ok(());
                }

                self.send_req("/boot-source", "PUT", Some(&config.boot_source)).await?;

                for drive in &config.drives {
                    self.send_req(format!("/drives/{}", drive.drive_id).as_str(), "PUT", Some(drive))
                        .await?;
                }

                self.send_req("/machine-config", "PUT", Some(&config.machine_configuration))
                    .await?;

                if let Some(cpu_template) = &config.cpu_template {
                    self.send_req("/cpu-config", "PUT", Some(cpu_template)).await?;
                }

                for network_interface in &config.network_interfaces {
                    self.send_req(
                        format!("/network-interfaces/{}", network_interface.iface_id).as_str(),
                        "PUT",
                        Some(network_interface),
                    )
                    .await?;
                }

                if let Some(balloon) = &config.balloon {
                    self.send_req("/balloon", "PUT", Some(balloon)).await?;
                }

                if let Some(vsock) = &config.vsock {
                    self.send_req("/vsock", "PUT", Some(vsock)).await?;
                }

                if let Some(logger) = &config.logger {
                    self.send_req("/logger", "PUT", Some(logger)).await?;
                }

                if let Some(metrics) = &config.metrics {
                    self.send_req("/metrics", "PUT", Some(metrics)).await?;
                }

                if let Some(mmds_configuration) = &config.mmds_configuration {
                    self.send_req("/mmds/config", "PUT", Some(mmds_configuration)).await?;
                }

                if let Some(entropy) = &config.entropy {
                    self.send_req("/entropy", "PUT", Some(entropy)).await?;
                }

                self.send_req(
                    "/actions",
                    "PUT",
                    Some(VmAction {
                        action_type: VmActionType::InstanceStart,
                    }),
                )
                .await?;
            }
            VmConfiguration::FromSnapshot(config) => {
                if let Some(logger) = &config.logger {
                    self.send_req("/logger", "PUT", Some(logger)).await?;
                }
                if let Some(metrics) = &config.metrics {
                    self.send_req("/metrics", "PUT", Some(metrics)).await?;
                }
                self.send_req("/snapshot/load", "PUT", Some(&config.load_snapshot))
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn shutdown(
        &mut self,
        shutdown_methods: Vec<VmShutdownMethod>,
        timeout: Duration,
    ) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        let mut last_result = Ok(());

        for shutdown_method in shutdown_methods {
            last_result = match shutdown_method {
                VmShutdownMethod::CtrlAltDel => self
                    .vmm_process
                    .send_ctrl_alt_del()
                    .await
                    .map_err(VmError::ProcessError),
                VmShutdownMethod::PauseThenKill => {
                    self.send_req(
                        "/vm",
                        "PATCH",
                        Some(VmUpdateState {
                            state: VmStateForUpdate::Paused,
                        }),
                    )
                    .await
                }
                VmShutdownMethod::WriteRebootToStdin(mut stdin) => {
                    stdin.write_all(b"reboot\n").await.map_err(VmError::IoError)?;
                    stdin.flush().await.map_err(VmError::IoError)
                }
                VmShutdownMethod::Kill => self.vmm_process.send_sigkill().map_err(VmError::ProcessError),
            };

            if last_result.is_ok() {
                last_result = tokio::time::timeout(timeout, self.vmm_process.wait_for_exit())
                    .await
                    .map_err(|_| VmError::Timeout)
                    .map(|_| ());
            }

            if last_result.is_ok() {
                break;
            }
        }

        if last_result.is_err() {
            return last_result;
        }

        Ok(())
    }

    pub async fn cleanup(&mut self, cleanup_options: VmCleanupOptions) -> Result<(), VmError> {
        self.ensure_exited_or_crashed()?;
        self.vmm_process.cleanup().await.map_err(VmError::ProcessError)?;

        if let Some(ref log_path) = self.standard_paths.log_path {
            if cleanup_options.remove_logs {
                tokio::fs::remove_file(log_path).await.map_err(VmError::IoError)?;
            }
        }

        if let Some(ref metrics_path) = self.standard_paths.metrics_path {
            if cleanup_options.remove_metrics {
                tokio::fs::remove_file(metrics_path).await.map_err(VmError::IoError)?;
            }
        }

        if let Some(ref multiplexer_path) = self.standard_paths.vsock_multiplexer_path {
            if cleanup_options.remove_vsock {
                tokio::fs::remove_file(multiplexer_path)
                    .await
                    .map_err(VmError::IoError)?;

                for listener_path in &self.standard_paths.vsock_listener_paths {
                    tokio::fs::remove_file(listener_path).await.map_err(VmError::IoError)?;
                }
            }
        }

        Ok(())
    }

    pub fn take_pipes(&mut self) -> Result<VmmProcessPipes, VmError> {
        self.ensure_paused_or_running()?;
        self.vmm_process.take_pipes().map_err(VmError::ProcessError)
    }

    pub fn standard_paths(&self) -> &VmStandardPaths {
        &self.standard_paths
    }

    pub fn standard_paths_mut(&mut self) -> &mut VmStandardPaths {
        &mut self.standard_paths
    }

    pub fn inner_to_outer_path(&self, inner_path: impl AsRef<Path>) -> PathBuf {
        self.vmm_process.inner_to_outer_path(inner_path)
    }

    pub async fn api_get_info(&mut self) -> Result<VmInfo, VmError> {
        self.ensure_paused_or_running()?;
        self.send_req_with_resp("/", "GET", None::<i32>).await
    }

    pub async fn api_flush_metrics(&mut self) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req(
            "/actions",
            "PUT",
            Some(VmAction {
                action_type: VmActionType::FlushMetrics,
            }),
        )
        .await
    }

    pub async fn api_get_balloon(&mut self) -> Result<VmBalloon, VmError> {
        self.ensure_paused_or_running()?;
        self.send_req_with_resp("/balloon", "GET", None::<i32>).await
    }

    pub async fn api_update_balloon(&mut self, update_balloon: VmUpdateBalloon) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req("/balloon", "PATCH", Some(update_balloon)).await
    }

    pub async fn api_get_balloon_statistics(&mut self) -> Result<VmBalloonStatistics, VmError> {
        self.ensure_state(VmState::Running)?;
        self.send_req_with_resp("/balloon/statistics", "GET", None::<i32>).await
    }

    pub async fn api_update_balloon_statistics(
        &mut self,
        update_balloon_statistics: VmUpdateBalloonStatistics,
    ) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req("/balloon/statistics", "PATCH", Some(update_balloon_statistics))
            .await
    }

    pub async fn api_update_drive(&mut self, update_drive: VmUpdateDrive) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req(
            format!("/drives/{}", update_drive.drive_id).as_str(),
            "PATCH",
            Some(update_drive),
        )
        .await
    }

    pub async fn api_update_network_interface(
        &mut self,
        update_network_interface: VmUpdateNetworkInterface,
    ) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req(
            format!("/network-interfaces/{}", update_network_interface.iface_id).as_str(),
            "PATCH",
            Some(update_network_interface),
        )
        .await
    }

    pub async fn api_get_machine_configuration(&mut self) -> Result<VmMachineConfiguration, VmError> {
        self.ensure_paused_or_running()?;
        self.send_req_with_resp("/machine-config", "GET", None::<i32>).await
    }

    pub async fn api_create_snapshot(&mut self, create_snapshot: VmCreateSnapshot) -> Result<VmSnapshotPaths, VmError> {
        self.ensure_state(VmState::Paused)?;
        self.send_req("/snapshot/create", "PUT", Some(&create_snapshot)).await?;
        Ok(VmSnapshotPaths {
            snapshot_path: self.vmm_process.inner_to_outer_path(create_snapshot.snapshot_path),
            mem_file_path: self.vmm_process.inner_to_outer_path(create_snapshot.mem_file_path),
        })
    }

    pub async fn api_get_firecracker_version(&mut self) -> Result<VmFirecrackerVersion, VmError> {
        self.ensure_paused_or_running()?;
        self.send_req_with_resp("/version", "GET", None::<i32>).await
    }

    pub async fn api_get_effective_configuration(&mut self) -> Result<VmEffectiveConfiguration, VmError> {
        self.ensure_paused_or_running()?;
        let fetched_configuration = self.send_req_with_resp("/vm/config", "GET", None::<i32>).await?;
        self.is_paused = false;
        Ok(fetched_configuration)
    }

    pub async fn api_pause(&mut self) -> Result<(), VmError> {
        self.ensure_state(VmState::Running)?;
        self.send_req(
            "/vm",
            "PATCH",
            Some(VmUpdateState {
                state: VmStateForUpdate::Paused,
            }),
        )
        .await?;
        self.is_paused = true;
        Ok(())
    }

    pub async fn api_resume(&mut self) -> Result<(), VmError> {
        self.ensure_state(VmState::Paused)?;
        self.send_req(
            "/vm",
            "PATCH",
            Some(VmUpdateState {
                state: VmStateForUpdate::Resumed,
            }),
        )
        .await?;
        self.is_paused = false;
        Ok(())
    }

    pub async fn api_create_mmds(&mut self, value: &serde_json::Value) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req("/mmds", "PUT", Some(value)).await
    }

    pub async fn api_update_mmds(&mut self, value: &serde_json::Value) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        self.send_req("/mmds", "PATCH", Some(value)).await
    }

    pub async fn api_get_mmds(&mut self) -> Result<serde_json::Value, VmError> {
        self.ensure_paused_or_running()?;
        self.send_req_with_resp("/mmds", "GET", None::<i32>).await
    }

    pub async fn api_custom_request(
        &mut self,
        route: impl AsRef<str>,
        request: Request<Full<Bytes>>,
        new_is_paused: Option<bool>,
    ) -> Result<Response<Incoming>, VmError> {
        self.ensure_paused_or_running()?;
        let response = self
            .vmm_process
            .send_api_request(route, request)
            .await
            .map_err(VmError::ProcessError)?;
        if let Some(new_is_paused) = new_is_paused {
            self.is_paused = new_is_paused;
        }
        Ok(response)
    }

    fn ensure_state(&mut self, expected_state: VmState) -> Result<(), VmError> {
        self.ensure_states(vec![expected_state])
    }

    fn ensure_paused_or_running(&mut self) -> Result<(), VmError> {
        self.ensure_states(vec![VmState::Running, VmState::Paused])
    }

    fn ensure_states(&mut self, expected_states: Vec<VmState>) -> Result<(), VmError> {
        let state = self.state();
        if !expected_states.contains(&state) {
            return Err(VmError::ExpectedState {
                expected: expected_states,
                actual: state,
            });
        }
        Ok(())
    }

    fn ensure_exited_or_crashed(&mut self) -> Result<(), VmError> {
        let state = self.state();
        if let VmState::Crashed(_) = state {
            return Ok(());
        }
        if state == VmState::Exited {
            return Ok(());
        }
        Err(VmError::ExpectedExitedOrCrashed { actual: state })
    }

    async fn send_req(
        &mut self,
        route: &str,
        method: &str,
        request_body: Option<impl Serialize>,
    ) -> Result<(), VmError> {
        let response_json: String = self.send_req_core(route, method, request_body).await?;
        if response_json.trim().is_empty() {
            Ok(())
        } else {
            Err(VmError::ApiResponseExpectedEmpty(response_json))
        }
    }

    async fn send_req_with_resp<Resp: DeserializeOwned>(
        &mut self,
        route: &str,
        method: &str,
        request_body: Option<impl Serialize>,
    ) -> Result<Resp, VmError> {
        let response_json = self.send_req_core(route, method, request_body).await?;
        serde_json::from_str(&response_json).map_err(VmError::SerdeError)
    }

    async fn send_req_core(
        &mut self,
        route: &str,
        method: &str,
        request_body: Option<impl Serialize>,
    ) -> Result<String, VmError> {
        let request_builder = Request::builder().method(method);
        let request = match request_body {
            Some(body) => {
                let request_json = serde_json::to_string(&body).map_err(VmError::SerdeError)?;
                request_builder
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(request_json)))
            }
            None => request_builder.body(Full::new(Bytes::new())),
        }
        .map_err(VmError::ApiRequestNotConstructed)?;
        let mut response = self
            .vmm_process
            .send_api_request(route, request)
            .await
            .map_err(VmError::ProcessError)?;
        let response_json = response
            .recv_to_string()
            .await
            .map_err(VmError::ApiResponseCouldNotBeReceived)?;

        if !response.status().is_success() {
            let api_error: VmApiError = serde_json::from_str(&response_json).map_err(VmError::SerdeError)?;
            return Err(VmError::ApiRespondedWithFault {
                status_code: response.status(),
                fault_message: api_error.fault_message,
            });
        }

        Ok(response_json)
    }
}

async fn prepare_file(path: &PathBuf, only_tree: bool) -> Result<(), VmError> {
    if let Some(parent_path) = path.parent() {
        fs::create_dir_all(parent_path).await.map_err(VmError::IoError)?;
    }

    if !only_tree {
        fs::File::create(path).await.map_err(VmError::IoError)?;
    }

    Ok(())
}
