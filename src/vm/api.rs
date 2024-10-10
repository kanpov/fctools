use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use serde::{de::DeserializeOwned, Serialize};

use crate::{executor::VmmExecutor, fs_backend::FsBackend, process::HyperResponseExt, shell_spawner::ShellSpawner};

use super::{
    configuration::VmConfigurationData,
    models::{
        BalloonDevice, BalloonStatistics, CreateSnapshot, EffectiveConfiguration, Info, LoadSnapshot,
        MachineConfiguration, ReprAction, ReprActionType, ReprApiError, ReprFirecrackerVersion, ReprInfo, ReprIsPaused,
        ReprUpdateState, ReprUpdatedState, UpdateBalloonDevice, UpdateBalloonStatistics, UpdateDrive,
        UpdateNetworkInterface,
    },
    snapshot::SnapshotData,
    Vm, VmError, VmState,
};

/// An extension to Vm providing up-to-date, exhaustive and easy-to-use bindings to the Firecracker Management API.
/// If the bindings here prove to be in some way inadequate, api_custom_request allows you to also call the Management
/// API with an arbitrary HTTP request, although bypassing some safeguards imposed by the provided bindings.
#[async_trait]
pub trait VmApi {
    async fn api_custom_request(
        &mut self,
        route: impl AsRef<str> + Send,
        request: Request<Full<Bytes>>,
        new_is_paused: Option<bool>,
    ) -> Result<Response<Incoming>, VmError>;

    async fn api_get_info(&mut self) -> Result<Info, VmError>;

    async fn api_flush_metrics(&mut self) -> Result<(), VmError>;

    async fn api_get_balloon_device(&mut self) -> Result<BalloonDevice, VmError>;

    async fn api_update_balloon_device(&mut self, update_balloon: UpdateBalloonDevice) -> Result<(), VmError>;

    async fn api_get_balloon_statistics(&mut self) -> Result<BalloonStatistics, VmError>;

    async fn api_update_balloon_statistics(
        &mut self,
        update_balloon_statistics: UpdateBalloonStatistics,
    ) -> Result<(), VmError>;

    async fn api_update_drive(&mut self, update_drive: UpdateDrive) -> Result<(), VmError>;

    async fn api_update_network_interface(
        &mut self,
        update_network_interface: UpdateNetworkInterface,
    ) -> Result<(), VmError>;

    async fn api_get_machine_configuration(&mut self) -> Result<MachineConfiguration, VmError>;

    async fn api_create_snapshot(&mut self, create_snapshot: CreateSnapshot) -> Result<SnapshotData, VmError>;

    async fn api_get_firecracker_version(&mut self) -> Result<String, VmError>;

    async fn api_get_effective_configuration(&mut self) -> Result<EffectiveConfiguration, VmError>;

    async fn api_pause(&mut self) -> Result<(), VmError>;

    async fn api_resume(&mut self) -> Result<(), VmError>;

    async fn api_create_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmError>;

    async fn api_update_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmError>;

    async fn api_get_mmds<T: DeserializeOwned>(&mut self) -> Result<T, VmError>;

    async fn api_create_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmError>;

    async fn api_update_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmError>;

    async fn api_get_mmds_untyped(&mut self) -> Result<serde_json::Value, VmError>;
}

#[async_trait]
impl<E: VmmExecutor, S: ShellSpawner, F: FsBackend> VmApi for Vm<E, S, F> {
    async fn api_custom_request(
        &mut self,
        route: impl AsRef<str> + Send,
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

    async fn api_get_info(&mut self) -> Result<Info, VmError> {
        self.ensure_paused_or_running()?;
        let repr: ReprInfo = send_api_request_with_response(self, "/", "GET", None::<i32>).await?;
        Ok(Info {
            id: repr.id,
            is_paused: repr.is_paused == ReprIsPaused::Paused,
            vmm_version: repr.vmm_version,
            app_name: repr.app_name,
        })
    }

    async fn api_flush_metrics(&mut self) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(
            self,
            "/actions",
            "PUT",
            Some(ReprAction {
                action_type: ReprActionType::FlushMetrics,
            }),
        )
        .await
    }

    async fn api_get_balloon_device(&mut self) -> Result<BalloonDevice, VmError> {
        self.ensure_paused_or_running()?;
        send_api_request_with_response(self, "/balloon", "GET", None::<i32>).await
    }

    async fn api_update_balloon_device(&mut self, update_balloon: UpdateBalloonDevice) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/balloon", "PATCH", Some(update_balloon)).await
    }

    async fn api_get_balloon_statistics(&mut self) -> Result<BalloonStatistics, VmError> {
        self.ensure_state(VmState::Running)?;
        send_api_request_with_response(self, "/balloon/statistics", "GET", None::<i32>).await
    }

    async fn api_update_balloon_statistics(
        &mut self,
        update_balloon_statistics: UpdateBalloonStatistics,
    ) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/balloon/statistics", "PATCH", Some(update_balloon_statistics)).await
    }

    async fn api_update_drive(&mut self, update_drive: UpdateDrive) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(
            self,
            format!("/drives/{}", update_drive.drive_id).as_str(),
            "PATCH",
            Some(update_drive),
        )
        .await
    }

    async fn api_update_network_interface(
        &mut self,
        update_network_interface: UpdateNetworkInterface,
    ) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(
            self,
            format!("/network-interfaces/{}", update_network_interface.iface_id).as_str(),
            "PATCH",
            Some(update_network_interface),
        )
        .await
    }

    async fn api_get_machine_configuration(&mut self) -> Result<MachineConfiguration, VmError> {
        self.ensure_paused_or_running()?;
        send_api_request_with_response(self, "/machine-config", "GET", None::<i32>).await
    }

    async fn api_create_snapshot(&mut self, create_snapshot: CreateSnapshot) -> Result<SnapshotData, VmError> {
        self.ensure_state(VmState::Paused)?;
        send_api_request(self, "/snapshot/create", "PUT", Some(&create_snapshot)).await?;
        let snapshot_path = self.vmm_process.inner_to_outer_path(create_snapshot.snapshot_path);
        let mem_file_path = self.vmm_process.inner_to_outer_path(create_snapshot.mem_file_path);
        if !self.executor_traceless {
            self.snapshot_traces.push(snapshot_path.clone());
            self.snapshot_traces.push(mem_file_path.clone());
        }

        Ok(SnapshotData {
            snapshot_path,
            mem_file_path,
            configuration_data: self.original_configuration_data.clone(),
        })
    }

    async fn api_get_firecracker_version(&mut self) -> Result<String, VmError> {
        self.ensure_paused_or_running()?;
        Ok(
            send_api_request_with_response::<ReprFirecrackerVersion, _, _, _>(self, "/version", "GET", None::<i32>)
                .await?
                .firecracker_version,
        )
    }

    async fn api_get_effective_configuration(&mut self) -> Result<EffectiveConfiguration, VmError> {
        self.ensure_paused_or_running()?;
        send_api_request_with_response(self, "/vm/config", "GET", None::<i32>).await
    }

    async fn api_pause(&mut self) -> Result<(), VmError> {
        self.ensure_state(VmState::Running)?;
        send_api_request(
            self,
            "/vm",
            "PATCH",
            Some(ReprUpdateState {
                state: ReprUpdatedState::Paused,
            }),
        )
        .await?;
        self.is_paused = true;
        Ok(())
    }

    async fn api_resume(&mut self) -> Result<(), VmError> {
        self.ensure_state(VmState::Paused)?;
        send_api_request(
            self,
            "/vm",
            "PATCH",
            Some(ReprUpdateState {
                state: ReprUpdatedState::Resumed,
            }),
        )
        .await?;
        self.is_paused = false;
        Ok(())
    }

    async fn api_create_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/mmds", "PUT", Some(value)).await
    }

    async fn api_update_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/mmds", "PATCH", Some(value)).await
    }

    async fn api_get_mmds<T: DeserializeOwned>(&mut self) -> Result<T, VmError> {
        self.ensure_paused_or_running()?;
        send_api_request_with_response(self, "/mmds", "GET", None::<i32>).await
    }

    async fn api_create_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/mmds", "PUT", Some(value)).await
    }

    async fn api_update_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmError> {
        self.ensure_paused_or_running()?;
        send_api_request(self, "/mmds", "PATCH", Some(value)).await
    }

    async fn api_get_mmds_untyped(&mut self) -> Result<serde_json::Value, VmError> {
        self.ensure_paused_or_running()?;
        send_api_request_with_response(self, "/mmds", "GET", None::<i32>).await
    }
}

pub(super) async fn init_new<E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    data: VmConfigurationData,
) -> Result<(), VmError> {
    send_api_request(vm, "/boot-source", "PUT", Some(&data.boot_source)).await?;

    for drive in &data.drives {
        send_api_request(vm, format!("/drives/{}", drive.drive_id).as_str(), "PUT", Some(drive)).await?;
    }

    send_api_request(vm, "/machine-config", "PUT", Some(&data.machine_configuration)).await?;

    if let Some(ref cpu_template) = data.cpu_template {
        send_api_request(vm, "/cpu-config", "PUT", Some(cpu_template)).await?;
    }

    for network_interface in &data.network_interfaces {
        send_api_request(
            vm,
            format!("/network-interfaces/{}", network_interface.iface_id).as_str(),
            "PUT",
            Some(network_interface),
        )
        .await?;
    }

    if let Some(ref balloon) = data.balloon_device {
        send_api_request(vm, "/balloon", "PUT", Some(balloon)).await?;
    }

    if let Some(ref vsock) = data.vsock_device {
        send_api_request(vm, "/vsock", "PUT", Some(vsock)).await?;
    }

    if let Some(ref logger) = data.logger_system {
        send_api_request(vm, "/logger", "PUT", Some(logger)).await?;
    }

    if let Some(ref metrics) = data.metrics_system {
        send_api_request(vm, "/metrics", "PUT", Some(metrics)).await?;
    }

    if let Some(ref mmds_configuration) = data.mmds_configuration {
        send_api_request(vm, "/mmds/config", "PUT", Some(mmds_configuration)).await?;
    }

    if let Some(ref entropy) = data.entropy_device {
        send_api_request(vm, "/entropy", "PUT", Some(entropy)).await?;
    }

    send_api_request(
        vm,
        "/actions",
        "PUT",
        Some(ReprAction {
            action_type: ReprActionType::InstanceStart,
        }),
    )
    .await
}

pub(super) async fn init_restored_from_snapshot<E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    data: VmConfigurationData,
    load_snapshot: LoadSnapshot,
) -> Result<(), VmError> {
    if let Some(ref logger) = data.logger_system {
        send_api_request(vm, "/logger", "PUT", Some(logger)).await?;
    }

    if let Some(ref metrics_system) = data.metrics_system {
        send_api_request(vm, "/metrics", "PUT", Some(metrics_system)).await?;
    }

    send_api_request(vm, "/snapshot/load", "PUT", Some(&load_snapshot)).await
}

async fn send_api_request<E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    route: &str,
    method: &str,
    request_body: Option<impl Serialize>,
) -> Result<(), VmError> {
    let response_json: String = send_api_request_internal(vm, route, method, request_body).await?;
    if response_json.trim().is_empty() {
        Ok(())
    } else {
        Err(VmError::ApiResponseExpectedEmpty(response_json))
    }
}

async fn send_api_request_with_response<Resp: DeserializeOwned, E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    route: &str,
    method: &str,
    request_body: Option<impl Serialize>,
) -> Result<Resp, VmError> {
    let response_json = send_api_request_internal(vm, route, method, request_body).await?;
    serde_json::from_str(&response_json).map_err(VmError::SerdeError)
}

async fn send_api_request_internal<E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
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
    let mut response = vm
        .vmm_process
        .send_api_request(route, request)
        .await
        .map_err(VmError::ProcessError)?;
    let response_json = response
        .recv_to_string()
        .await
        .map_err(VmError::ApiResponseCouldNotBeReceived)?;

    if !response.status().is_success() {
        let api_error: ReprApiError = serde_json::from_str(&response_json).map_err(VmError::SerdeError)?;
        return Err(VmError::ApiRespondedWithFault {
            status_code: response.status(),
            fault_message: api_error.fault_message,
        });
    }

    Ok(response_json)
}
