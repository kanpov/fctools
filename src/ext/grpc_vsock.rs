use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, task::Poll};

use http::Uri;
use hyper_client_sockets::HyperFirecrackerStream;
use tonic::transport::{Channel, Endpoint};

use crate::{executor::VmmExecutor, fs_backend::FsBackend, shell_spawner::ShellSpawner, vm::Vm};

/// An error emitted by the gRPC-over-vsock extension.
#[derive(Debug, thiserror::Error)]
pub enum VsockGrpcError {
    #[error("A vsock device was not configured for this VM")]
    VsockNotConfigured,
    #[error("The provided address was rejected as an Endpoint by tonic: `{0}`")]
    ProvidedAddressRejected(tonic::transport::Error),
    #[error("The connection process failed: `{0}`")]
    ConnectionFailed(tonic::transport::Error),
}

/// An extension that allows connecting to guest applications that expose a gRPC server being tunneled over
/// the Firecracker vsock device. The established tonic Channel-s can be used with codegen or any other type
/// of tonic client.
#[async_trait::async_trait]
pub trait VsockGrpcExt {
    /// Connect to a guest port over gRPC eagerly, i.e. by establishing the connection right away.
    /// configure_endpoint can be used as a function to customize Endpoint options via its builder.
    async fn vsock_connect_over_grpc(
        &self,
        guest_port: u32,
        configure_endpoint: impl (FnOnce(Endpoint) -> Endpoint) + Send,
    ) -> Result<Channel, VsockGrpcError>;

    /// Connect to a guest port over gRPC lazily, i.e. not actually establishing the connection until
    /// first usage of the Channel.
    /// configure_endpoint can be used as a function to customize Endpoint options via its builder.
    fn vsock_lazily_connect_over_grpc(
        &self,
        guest_port: u32,
        configure_endpoint: impl (FnOnce(Endpoint) -> Endpoint) + Send,
    ) -> Result<Channel, VsockGrpcError>;
}

#[async_trait::async_trait]
impl<E: VmmExecutor, S: ShellSpawner, F: FsBackend> VsockGrpcExt for Vm<E, S, F> {
    async fn vsock_connect_over_grpc(
        &self,
        guest_port: u32,
        configure_endpoint: impl (FnOnce(Endpoint) -> Endpoint) + Send,
    ) -> Result<Channel, VsockGrpcError> {
        let (endpoint, service) = create_endpoint_and_service(&self, guest_port, configure_endpoint)?;
        Ok(endpoint
            .connect_with_connector(service)
            .await
            .map_err(VsockGrpcError::ConnectionFailed)?)
    }

    fn vsock_lazily_connect_over_grpc(
        &self,
        guest_port: u32,
        configure_endpoint: impl (FnOnce(Endpoint) -> Endpoint) + Send,
    ) -> Result<Channel, VsockGrpcError> {
        let (endpoint, service) = create_endpoint_and_service(&self, guest_port, configure_endpoint)?;
        Ok(endpoint.connect_with_connector_lazy(service))
    }
}

#[inline]
fn create_endpoint_and_service<E: VmmExecutor, S: ShellSpawner, F: FsBackend>(
    vm: &Vm<E, S, F>,
    guest_port: u32,
    configure_endpoint: impl (FnOnce(Endpoint) -> Endpoint) + Send,
) -> Result<(Endpoint, FirecrackerTowerService), VsockGrpcError> {
    let uds_path = vm
        .get_accessible_paths()
        .vsock_multiplexer_path
        .clone()
        .ok_or(VsockGrpcError::VsockNotConfigured)?;

    let mut endpoint =
        Endpoint::try_from(format!("http://[::1]:{guest_port}")).map_err(VsockGrpcError::ProvidedAddressRejected)?;
    endpoint = configure_endpoint(endpoint);

    let service = FirecrackerTowerService {
        guest_port,
        uds_path: Arc::new(uds_path),
    };

    Ok((endpoint, service))
}

struct FirecrackerTowerService {
    guest_port: u32,
    uds_path: Arc<PathBuf>,
}

impl tower_service::Service<Uri> for FirecrackerTowerService {
    type Response = HyperFirecrackerStream;

    type Error = std::io::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Uri) -> Self::Future {
        let uds_path = self.uds_path.clone();
        let guest_port = self.guest_port;

        Box::pin(async move {
            let stream = HyperFirecrackerStream::connect(uds_path.as_ref(), guest_port).await?;
            Ok(stream)
        })
    }
}
