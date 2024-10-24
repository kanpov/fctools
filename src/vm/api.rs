use std::future::Future;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    fs_backend::FsBackend,
    process_spawner::ProcessSpawner,
    vmm::{
        executor::VmmExecutor,
        process::{HyperResponseExt, VmmProcessError},
    },
};

use super::VmStateCheckError;
use super::{
    configuration::VmConfigurationData,
    models::{
        BalloonDevice, BalloonStatistics, CreateSnapshot, EffectiveConfiguration, Info, LoadSnapshot,
        MachineConfiguration, ReprAction, ReprActionType, ReprApiError, ReprFirecrackerVersion, ReprInfo, ReprIsPaused,
        ReprUpdateState, ReprUpdatedState, UpdateBalloonDevice, UpdateBalloonStatistics, UpdateDrive,
        UpdateNetworkInterface,
    },
    snapshot::SnapshotData,
    Vm, VmState,
};

/// An error that can be emitted by the [VmApi] Firecracker Management API bindings.
#[derive(Debug, thiserror::Error)]
pub enum VmApiError {
    #[error("Serializing or deserializing JSON data via serde-json failed: `{0}`")]
    SerdeError(serde_json::Error),
    #[error("The API returned an unsuccessful HTTP response with the `{status_code}` status: `{fault_message}`")]
    ReceivedErrorResponse {
        status_code: StatusCode,
        fault_message: String,
    },
    #[error("Building the HTTP request for the API failed: `{0}`")]
    RequestBuildError(http::Error),
    #[error("Sending the HTTP process via the VMM process failed: `{0}`")]
    ConnectionError(VmmProcessError),
    #[error("The HTTP response body from the API could not be received over the established connection: `{0}")]
    ResponseBodyReceiveError(hyper::Error),
    #[error("The HTTP response body from the API was presumed empty but actually contains data: `{0}`")]
    ResponseBodyContainsUnexpectedData(String),
    #[error("A state check of the VM failed: `{0}`")]
    StateCheckError(VmStateCheckError),
}

/// An extension to [Vm] providing up-to-date, exhaustive and easy-to-use bindings to the Firecracker Management API.
/// If the bindings here prove to be in some way inadequate, [VmApi::api_custom_request] allows you to also call the Management
/// API with an arbitrary HTTP request, though while bypassing some safeguards imposed by the provided bindings.
pub trait VmApi {
    fn api_custom_request(
        &mut self,
        route: impl AsRef<str> + Send,
        request: Request<Full<Bytes>>,
        new_is_paused: Option<bool>,
    ) -> impl Future<Output = Result<Response<Incoming>, VmApiError>> + Send;

    fn api_get_info(&mut self) -> impl Future<Output = Result<Info, VmApiError>> + Send;

    fn api_flush_metrics(&mut self) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_get_balloon_device(&mut self) -> impl Future<Output = Result<BalloonDevice, VmApiError>> + Send;

    fn api_update_balloon_device(
        &mut self,
        update_balloon: UpdateBalloonDevice,
    ) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_get_balloon_statistics(&mut self) -> impl Future<Output = Result<BalloonStatistics, VmApiError>> + Send;

    fn api_update_balloon_statistics(
        &mut self,
        update_balloon_statistics: UpdateBalloonStatistics,
    ) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_update_drive(&mut self, update_drive: UpdateDrive) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_update_network_interface(
        &mut self,
        update_network_interface: UpdateNetworkInterface,
    ) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_get_machine_configuration(
        &mut self,
    ) -> impl Future<Output = Result<MachineConfiguration, VmApiError>> + Send;

    fn api_create_snapshot(
        &mut self,
        create_snapshot: CreateSnapshot,
    ) -> impl Future<Output = Result<SnapshotData, VmApiError>> + Send;

    fn api_get_firecracker_version(&mut self) -> impl Future<Output = Result<String, VmApiError>> + Send;

    fn api_get_effective_configuration(
        &mut self,
    ) -> impl Future<Output = Result<EffectiveConfiguration, VmApiError>> + Send;

    fn api_pause(&mut self) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_resume(&mut self) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_create_mmds<T: Serialize + Send>(&mut self, value: T)
        -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_update_mmds<T: Serialize + Send>(&mut self, value: T)
        -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_get_mmds<T: DeserializeOwned>(&mut self) -> impl Future<Output = Result<T, VmApiError>> + Send;

    fn api_create_mmds_untyped(
        &mut self,
        value: &serde_json::Value,
    ) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_update_mmds_untyped(
        &mut self,
        value: &serde_json::Value,
    ) -> impl Future<Output = Result<(), VmApiError>> + Send;

    fn api_get_mmds_untyped(&mut self) -> impl Future<Output = Result<serde_json::Value, VmApiError>> + Send;
}

impl<E: VmmExecutor, S: ProcessSpawner, F: FsBackend> VmApi for Vm<E, S, F> {
    async fn api_custom_request(
        &mut self,
        route: impl AsRef<str> + Send,
        request: Request<Full<Bytes>>,
        new_is_paused: Option<bool>,
    ) -> Result<Response<Incoming>, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        let response = self
            .vmm_process
            .send_api_request(route, request)
            .await
            .map_err(VmApiError::ConnectionError)?;
        if let Some(new_is_paused) = new_is_paused {
            self.is_paused = new_is_paused;
        }

        Ok(response)
    }

    async fn api_get_info(&mut self) -> Result<Info, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        let repr: ReprInfo = send_api_request_with_response(self, "/", "GET", None::<i32>).await?;
        Ok(Info {
            id: repr.id,
            is_paused: repr.is_paused == ReprIsPaused::Paused,
            vmm_version: repr.vmm_version,
            app_name: repr.app_name,
        })
    }

    async fn api_flush_metrics(&mut self) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
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

    async fn api_get_balloon_device(&mut self) -> Result<BalloonDevice, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/balloon", "GET", None::<i32>).await
    }

    async fn api_update_balloon_device(&mut self, update_balloon: UpdateBalloonDevice) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/balloon", "PATCH", Some(update_balloon)).await
    }

    async fn api_get_balloon_statistics(&mut self) -> Result<BalloonStatistics, VmApiError> {
        self.ensure_state(VmState::Running)
            .map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/balloon/statistics", "GET", None::<i32>).await
    }

    async fn api_update_balloon_statistics(
        &mut self,
        update_balloon_statistics: UpdateBalloonStatistics,
    ) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/balloon/statistics", "PATCH", Some(update_balloon_statistics)).await
    }

    async fn api_update_drive(&mut self, update_drive: UpdateDrive) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
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
    ) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(
            self,
            format!("/network-interfaces/{}", update_network_interface.iface_id).as_str(),
            "PATCH",
            Some(update_network_interface),
        )
        .await
    }

    async fn api_get_machine_configuration(&mut self) -> Result<MachineConfiguration, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/machine-config", "GET", None::<i32>).await
    }

    async fn api_create_snapshot(&mut self, create_snapshot: CreateSnapshot) -> Result<SnapshotData, VmApiError> {
        self.ensure_state(VmState::Paused)
            .map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/snapshot/create", "PUT", Some(&create_snapshot)).await?;
        let snapshot_path = self.vmm_process.inner_to_outer_path(create_snapshot.snapshot_path);
        let mem_file_path = self.vmm_process.inner_to_outer_path(create_snapshot.mem_file_path);
        if !self.executor.is_traceless() {
            self.snapshot_traces.push(snapshot_path.clone());
            self.snapshot_traces.push(mem_file_path.clone());
        }

        Ok(SnapshotData {
            snapshot_path,
            mem_file_path,
            configuration_data: self.original_configuration_data.clone(),
        })
    }

    async fn api_get_firecracker_version(&mut self) -> Result<String, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        Ok(
            send_api_request_with_response::<ReprFirecrackerVersion, _, _, _>(self, "/version", "GET", None::<i32>)
                .await?
                .firecracker_version,
        )
    }

    async fn api_get_effective_configuration(&mut self) -> Result<EffectiveConfiguration, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/vm/config", "GET", None::<i32>).await
    }

    async fn api_pause(&mut self) -> Result<(), VmApiError> {
        self.ensure_state(VmState::Running)
            .map_err(VmApiError::StateCheckError)?;
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

    async fn api_resume(&mut self) -> Result<(), VmApiError> {
        self.ensure_state(VmState::Paused)
            .map_err(VmApiError::StateCheckError)?;
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

    async fn api_create_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/mmds", "PUT", Some(value)).await
    }

    async fn api_update_mmds<T: Serialize + Send>(&mut self, value: T) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/mmds", "PATCH", Some(value)).await
    }

    async fn api_get_mmds<T: DeserializeOwned>(&mut self) -> Result<T, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/mmds", "GET", None::<i32>).await
    }

    async fn api_create_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/mmds", "PUT", Some(value)).await
    }

    async fn api_update_mmds_untyped(&mut self, value: &serde_json::Value) -> Result<(), VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request(self, "/mmds", "PATCH", Some(value)).await
    }

    async fn api_get_mmds_untyped(&mut self) -> Result<serde_json::Value, VmApiError> {
        self.ensure_paused_or_running().map_err(VmApiError::StateCheckError)?;
        send_api_request_with_response(self, "/mmds", "GET", None::<i32>).await
    }
}

pub(super) async fn init_new<E: VmmExecutor, S: ProcessSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    data: VmConfigurationData,
) -> Result<(), VmApiError> {
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

pub(super) async fn init_restored_from_snapshot<E: VmmExecutor, S: ProcessSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    data: VmConfigurationData,
    load_snapshot: LoadSnapshot,
) -> Result<(), VmApiError> {
    if let Some(ref logger) = data.logger_system {
        send_api_request(vm, "/logger", "PUT", Some(logger)).await?;
    }

    if let Some(ref metrics_system) = data.metrics_system {
        send_api_request(vm, "/metrics", "PUT", Some(metrics_system)).await?;
    }

    send_api_request(vm, "/snapshot/load", "PUT", Some(&load_snapshot)).await
}

async fn send_api_request<E: VmmExecutor, S: ProcessSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    route: &str,
    method: &str,
    request_body: Option<impl Serialize>,
) -> Result<(), VmApiError> {
    let response_body: String = send_api_request_internal(vm, route, method, request_body).await?;
    if response_body.trim().is_empty() {
        Ok(())
    } else {
        Err(VmApiError::ResponseBodyContainsUnexpectedData(response_body))
    }
}

async fn send_api_request_with_response<Resp: DeserializeOwned, E: VmmExecutor, S: ProcessSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    route: &str,
    method: &str,
    request_body: Option<impl Serialize>,
) -> Result<Resp, VmApiError> {
    let response_json = send_api_request_internal(vm, route, method, request_body).await?;
    serde_json::from_str(&response_json).map_err(VmApiError::SerdeError)
}

async fn send_api_request_internal<E: VmmExecutor, S: ProcessSpawner, F: FsBackend>(
    vm: &mut Vm<E, S, F>,
    route: &str,
    method: &str,
    request_body: Option<impl Serialize>,
) -> Result<String, VmApiError> {
    let request_builder = Request::builder().method(method);
    let request = match request_body {
        Some(body) => {
            let request_json = serde_json::to_string(&body).map_err(VmApiError::SerdeError)?;
            request_builder
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(request_json)))
        }
        None => request_builder.body(Full::new(Bytes::new())),
    }
    .map_err(VmApiError::RequestBuildError)?;
    let mut response = vm
        .vmm_process
        .send_api_request(route, request)
        .await
        .map_err(VmApiError::ConnectionError)?;
    let response_json = response
        .recv_to_string()
        .await
        .map_err(VmApiError::ResponseBodyReceiveError)?;

    if !response.status().is_success() {
        let api_error: ReprApiError = serde_json::from_str(&response_json).map_err(VmApiError::SerdeError)?;
        return Err(VmApiError::ReceivedErrorResponse {
            status_code: response.status(),
            fault_message: api_error.fault_message,
        });
    }

    Ok(response_json)
}
