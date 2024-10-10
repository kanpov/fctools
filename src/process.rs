use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::{Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper_client_sockets::{HyperUnixConnector, UnixUriExt};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout},
    sync::OnceCell,
};

use crate::{
    executor::{
        arguments::ConfigurationFileOverride, force_chown, installation::VmmInstallation, VmmExecutor, VmmExecutorError,
    },
    fs_backend::FsBackend,
    shell_spawner::ShellSpawner,
};

/// A VMM process is layer 3 of FCTools: an abstraction that manages a VMM process. It is
/// tied to the given VMM executor E and shell spawner S.
#[derive(Debug)]
pub struct VmmProcess<E: VmmExecutor, S: ShellSpawner, F: FsBackend> {
    executor: Arc<E>,
    shell_spawner: Arc<S>,
    fs_backend: Arc<F>,
    installation: Arc<VmmInstallation>,
    child: Option<Child>,
    state: VmmProcessState,
    socket_path: Option<PathBuf>,
    hyper_client: OnceCell<Client<HyperUnixConnector, Full<Bytes>>>,
    outer_paths: Option<Vec<PathBuf>>,
}

/// Raw Tokio pipes of a VMM process: stdin, stdout and stderr. All must always be redirected
/// by the respective shell spawner implementation.
#[derive(Debug)]
pub struct VmmProcessPipes {
    /// The standard input pipe.
    pub stdin: ChildStdin,
    /// The standard output pipe.
    pub stdout: ChildStdout,
    /// The standard error pipe.
    pub stderr: ChildStderr,
}

/// The state of a VMM process.
/// Keep in mind: the VMM process lifecycle is not that of the VM! If the process has
/// started without a config file, API requests will need to be issued first in order
/// to start it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmmProcessState {
    /// The process hasn't started, and calling "prepare" is needed.
    AwaitingPrepare,
    /// The process hasn't started, a request to start it must be issued.
    AwaitingStart,
    /// The process is running without any faults.
    Started,
    /// The process has exited gracefully after a host-issued exit request.
    Exited,
    /// The process has crashed with the given non-zero exit status code.
    Crashed(ExitStatus),
}

impl std::fmt::Display for VmmProcessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmmProcessState::AwaitingPrepare => write!(f, "Awaiting prepare"),
            VmmProcessState::AwaitingStart => write!(f, "Awaiting start"),
            VmmProcessState::Started => write!(f, "Started"),
            VmmProcessState::Exited => write!(f, "Exited"),
            VmmProcessState::Crashed(exit_status) => write!(f, "Crashed with {exit_status}"),
        }
    }
}

/// Error caused during a VMM process operation.
#[derive(Debug, thiserror::Error)]
pub enum VmmProcessError {
    #[error("Expected the VMM process to have the `{expected}` state, but it actually had the `{actual}` state")]
    ExpectedState {
        expected: VmmProcessState,
        actual: VmmProcessState,
    },
    #[error("Expected the VMM process to either have exited or crashed, but it actually had the `{actual}` state")]
    ExpectedExitedOrCrashed { actual: VmmProcessState },
    #[error("The VMM process's API socket had been disabled yet a request to the socket was attempted")]
    SocketWasDisabled,
    #[error("Forcing chown of the API socket via the shell spawner failed: `{0}`")]
    CouldNotChownSocket(io::Error),
    #[error("An error occurred in the internal hyper-util HTTP connection pool: `{0}`")]
    HyperClientFailed(hyper_util::client::legacy::Error),
    #[error("Transmitting SIGKILL to the process failed: `{0}`")]
    SigkillFailed(io::Error),
    #[error("Building the Ctrl+Alt+Del HTTP request failed: `{0}`")]
    CtrlAltDelRequestNotBuilt(hyper::http::Error),
    #[error("The Ctrl+Alt+Del HTTP request returned an unsuccessful status code: `{0}`")]
    CtrlAltDelRequestFailed(StatusCode),
    #[error("Awaiting the process' exit failed: `{0}`")]
    WaitFailed(io::Error),
    #[error("The given route to the API socket could not be transformed to a Unix socket URI")]
    IncorrectSocketUri,
    #[error("The underlying VMM executor returned an error: `{0}`")]
    ExecutorError(VmmExecutorError),
    #[error(
        "Attempted to take out the process' pipes when they had already been taken or were redirected to /dev/null"
    )]
    PipesNulledOrAlreadyTaken,
}

impl<E: VmmExecutor, S: ShellSpawner, F: FsBackend> VmmProcess<E, S, F> {
    /// Create a new process instance while moving in its components.
    pub fn new(
        executor: E,
        shell_spawner: S,
        fs_backend: F,
        installation: VmmInstallation,
        outer_paths: Vec<PathBuf>,
    ) -> Self {
        Self::new_arced(
            Arc::new(executor),
            Arc::new(shell_spawner),
            Arc::new(fs_backend),
            Arc::new(installation),
            outer_paths,
        )
    }

    /// Create a new process instance without moving in its components, instead using Arc-s. This is recommended over moving in clones in
    /// all scenarios.
    pub fn new_arced(
        executor_arc: Arc<E>,
        shell_spawner_arc: Arc<S>,
        fs_backend_arc: Arc<F>,
        installation_arc: Arc<VmmInstallation>,
        outer_paths: Vec<PathBuf>,
    ) -> Self {
        let socket_path = executor_arc.get_socket_path(installation_arc.as_ref());
        Self {
            executor: executor_arc,
            shell_spawner: shell_spawner_arc,
            installation: installation_arc,
            fs_backend: fs_backend_arc,
            child: None,
            state: VmmProcessState::AwaitingPrepare,
            socket_path,
            hyper_client: OnceCell::new(),
            outer_paths: Some(outer_paths),
        }
    }

    /// Prepare the VM process environment. Allowed in AwaitingPrepare state, will result in AwaitingStart state.
    pub async fn prepare(&mut self) -> Result<HashMap<PathBuf, PathBuf>, VmmProcessError> {
        self.ensure_state(VmmProcessState::AwaitingPrepare)?;
        let path_mappings = self
            .executor
            .prepare(
                self.installation.as_ref(),
                self.shell_spawner.clone(),
                self.fs_backend.clone(),
                self.outer_paths
                    .take()
                    .expect("Outer paths cannot ever be none, unreachable"),
            )
            .await
            .map_err(VmmProcessError::ExecutorError)?;
        self.state = VmmProcessState::AwaitingStart;
        Ok(path_mappings)
    }

    /// Invoke the VM process. Allowed in AwaitingStart state, will result in Started state.
    pub async fn invoke(&mut self, config_override: ConfigurationFileOverride) -> Result<(), VmmProcessError> {
        self.ensure_state(VmmProcessState::AwaitingStart)?;
        self.child = Some(
            self.executor
                .invoke(self.installation.as_ref(), self.shell_spawner.clone(), config_override)
                .await
                .map_err(VmmProcessError::ExecutorError)?,
        );
        self.state = VmmProcessState::Started;
        Ok(())
    }

    /// Send a given request (without a URI being set) to the given route of the Firecracker API server.
    /// Allowed in Started state.
    pub async fn send_api_request(
        &mut self,
        route: impl AsRef<str>,
        mut request: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, VmmProcessError> {
        self.ensure_state(VmmProcessState::Started)?;
        let socket_path = self.socket_path.as_ref().ok_or(VmmProcessError::SocketWasDisabled)?;
        let hyper_client = self
            .hyper_client
            .get_or_try_init(|| async {
                force_chown(&socket_path, self.shell_spawner.as_ref())
                    .await
                    .map_err(VmmProcessError::ExecutorError)?;
                Ok(Client::builder(TokioExecutor::new()).build(HyperUnixConnector))
            })
            .await?;

        *request.uri_mut() = Uri::unix(socket_path, route).map_err(|_| VmmProcessError::IncorrectSocketUri)?;
        hyper_client
            .request(request)
            .await
            .map_err(VmmProcessError::HyperClientFailed)
    }

    /// Take out the stdout, stdin, stderr pipes of the underlying VM process. This can be only done once,
    /// if some code takes out the pipes, it now owns them for the remaining lifespan of the process.
    /// Allowed in Started state.
    pub fn take_pipes(&mut self) -> Result<VmmProcessPipes, VmmProcessError> {
        self.ensure_state(VmmProcessState::Started)?;
        let child_ref = self.child.as_mut().expect("No child while running");
        let stdin = child_ref
            .stdin
            .take()
            .ok_or(VmmProcessError::PipesNulledOrAlreadyTaken)?;
        let stdout = child_ref
            .stdout
            .take()
            .ok_or(VmmProcessError::PipesNulledOrAlreadyTaken)?;
        let stderr = child_ref
            .stderr
            .take()
            .ok_or(VmmProcessError::PipesNulledOrAlreadyTaken)?;
        Ok(VmmProcessPipes { stdin, stdout, stderr })
    }

    /// Gets the outer path to the VM process's socket, if one has been configured, via the executor.
    pub fn get_socket_path(&self) -> Option<PathBuf> {
        self.executor.get_socket_path(self.installation.as_ref())
    }

    /// Converts a given inner path to an outer path via the executor.
    pub fn inner_to_outer_path(&self, inner_path: impl AsRef<Path>) -> PathBuf {
        self.executor
            .inner_to_outer_path(self.installation.as_ref(), inner_path.as_ref())
    }

    /// Send a graceful shutdown request via Ctrl+Alt+Del to the VM process. Allowed on x86_64 as per Firecracker docs,
    /// on ARM either try to write "reboot\n" to stdin or pause the VM and SIGKILL it for a comparable effect.
    /// Allowed in Started state, will result in Exited state.
    pub async fn send_ctrl_alt_del(&mut self) -> Result<(), VmmProcessError> {
        let response = self
            .send_api_request(
                "/actions",
                Request::builder()
                    .method("PUT")
                    .body(Full::new(Bytes::from(r#"{"action_type": "SendCtrlAltDel"}"#)))
                    .map_err(VmmProcessError::CtrlAltDelRequestNotBuilt)?,
            )
            .await?;
        if !response.status().is_success() {
            return Err(VmmProcessError::CtrlAltDelRequestFailed(response.status()));
        }

        Ok(())
    }

    /// Send an immediate forceful shutdown request in the form of a SIGKILL signal to the VM process.
    /// Allowed in Started state, will result in Crashed state.
    pub fn send_sigkill(&mut self) -> Result<(), VmmProcessError> {
        self.ensure_state(VmmProcessState::Started)?;
        self.child
            .as_mut()
            .expect("No child while running")
            .start_kill()
            .map_err(VmmProcessError::SigkillFailed)
    }

    /// Wait until the VM process exits. Careful not to wait forever! Allowed in Started state, will result
    /// in either Exited or Crashed state, returning the exit status.
    pub async fn wait_for_exit(&mut self) -> Result<ExitStatus, VmmProcessError> {
        self.ensure_state(VmmProcessState::Started)?;
        self.child
            .as_mut()
            .expect("No child while running")
            .wait()
            .await
            .map_err(VmmProcessError::WaitFailed)
    }

    /// Returns the current state of the VM process. Needs mutability since the tracking will need to be updated.
    /// Allowed in any state.
    pub fn state(&mut self) -> VmmProcessState {
        self.update_state();
        self.state
    }

    /// Cleans up the VM process environment. Always call this as a sort of async drop mechanism! Allowed in Exited
    /// or Crashed state.
    pub async fn cleanup(&mut self) -> Result<(), VmmProcessError> {
        self.ensure_exited_or_crashed()?;
        self.executor
            .cleanup(
                self.installation.as_ref(),
                self.shell_spawner.clone(),
                self.fs_backend.clone(),
            )
            .await
            .map_err(VmmProcessError::ExecutorError)?;
        Ok(())
    }

    fn ensure_state(&mut self, expected: VmmProcessState) -> Result<(), VmmProcessError> {
        if self.state() != expected {
            return Err(VmmProcessError::ExpectedState {
                expected,
                actual: self.state,
            });
        }
        Ok(())
    }

    fn ensure_exited_or_crashed(&mut self) -> Result<(), VmmProcessError> {
        let state = self.state();
        if let VmmProcessState::Crashed(_) = state {
            return Ok(());
        }

        if state == VmmProcessState::Exited {
            return Ok(());
        }

        Err(VmmProcessError::ExpectedExitedOrCrashed { actual: state })
    }

    fn update_state(&mut self) {
        if let Some(ref mut child) = self.child {
            if let Ok(Some(exit_status)) = child.try_wait() {
                if exit_status.success() {
                    self.state = VmmProcessState::Exited;
                } else {
                    self.state = VmmProcessState::Crashed(exit_status);
                }
            }
        }
    }
}

/// An extension to a hyper Response<Incoming> (returned by the Firecracker API socket) that allows
/// easy streaming of the response body.
#[async_trait]
pub trait HyperResponseExt {
    /// Stream the entire response body into a byte buffer (BytesMut).
    async fn recv_to_buf(&mut self) -> Result<BytesMut, hyper::Error>;

    /// Stream the entire response body into an owned string.
    async fn recv_to_string(&mut self) -> Result<String, hyper::Error> {
        let buf = self.recv_to_buf().await?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

#[async_trait]
impl HyperResponseExt for Response<Incoming> {
    async fn recv_to_buf(&mut self) -> Result<BytesMut, hyper::Error> {
        let mut buf = BytesMut::with_capacity(self.body().size_hint().lower() as usize);
        while let Some(frame) = self.frame().await {
            if let Ok(bytes) = frame?.into_data() {
                buf.extend(bytes);
            }
        }
        Ok(buf)
    }
}
