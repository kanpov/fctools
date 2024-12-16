use std::{
    os::{
        fd::{AsRawFd, RawFd},
        unix::process::ExitStatusExt,
    },
    path::PathBuf,
    process::ExitStatus,
};

use crate::runtime::{Runtime, RuntimeAsyncFd, RuntimeExecutor, RuntimeFilesystem, RuntimeProcess};

/// A process handle is a thin abstraction over either an "attached" child process that is a [RuntimeProcess],
/// or a "detached" certain process that isn't a child and is controlled via a [RuntimeAsyncFd] wrapping a
/// Linux pidfd.
#[derive(Debug)]
pub struct ProcessHandle<P: RuntimeProcess>(ProcessHandleInner<P>);

/// The pipes that are extracted from a [ProcessHandle]. These can only be extracted from attached
/// [ProcessHandle]s that haven't had their pipes dropped to /dev/null.
#[derive(Debug)]
pub struct ProcessHandlePipes<P: RuntimeProcess> {
    pub stdout: P::Stdout,
    pub stderr: P::Stderr,
    pub stdin: P::Stdin,
}

/// An error that didn't allow the extraction of [ProcessHandlePipes] from a [ProcessHandle].
#[derive(Debug)]
pub enum ProcessHandlePipesError {
    ProcessIsDetached,
    PipesWereDropped,
    PipesWereAlreadyTaken,
}

impl std::error::Error for ProcessHandlePipesError {}

impl std::fmt::Display for ProcessHandlePipesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessHandlePipesError::ProcessIsDetached => write!(
                f,
                "The handle points to a detached process outside the PID namespace of the current one"
            ),
            ProcessHandlePipesError::PipesWereDropped => write!(f, "The pipes of the process were dropped"),
            ProcessHandlePipesError::PipesWereAlreadyTaken => {
                write!(f, "The pipes were already taken (given ownership of)")
            }
        }
    }
}

#[derive(Debug)]
enum ProcessHandleInner<P: RuntimeProcess> {
    Child {
        process: P,
        pipes_dropped: bool,
    },
    Pidfd {
        raw_pidfd: RawFd,
        exited_rx: futures_channel::oneshot::Receiver<ExitStatus>,
        exited: Option<ExitStatus>,
    },
}

impl<P: RuntimeProcess> ProcessHandle<P> {
    /// Create a [ProcessHandle] from a [RuntimeProcess] that is a child of the current process.
    pub fn with_child(process: P, pipes_dropped: bool) -> Self {
        Self(ProcessHandleInner::Child { process, pipes_dropped })
    }

    /// Try to create a [ProcessHandle] by allocating a pidfd for the given PID.
    pub fn with_pidfd<R: Runtime>(pid: i32, runtime: R) -> Result<Self, std::io::Error> {
        let pidfd = crate::sys::pidfd_open(pid)?;
        let raw_pidfd = pidfd.as_raw_fd();

        let (exited_tx, exited_rx) = futures_channel::oneshot::channel();
        let async_pidfd = runtime.filesystem().create_async_fd(pidfd)?;

        let _ = runtime.executor().spawn(async move {
            if async_pidfd.readable().await.is_ok() {
                let mut exit_status = ExitStatus::from_raw(0);

                if let Ok(content) = runtime
                    .filesystem()
                    .read_to_string(&PathBuf::from(format!("/proc/{pid}/stat")))
                    .await
                {
                    if let Some(status_raw) = content.split_whitespace().last().and_then(|value| value.parse().ok()) {
                        exit_status = ExitStatus::from_raw(status_raw);
                    }
                }

                let _ = exited_tx.send(exit_status);
            }
        });

        Ok(Self(ProcessHandleInner::Pidfd {
            raw_pidfd,
            exited_rx,
            exited: None,
        }))
    }

    /// Send a SIGKILL signal to the process.
    pub fn send_sigkill(&mut self) -> Result<(), std::io::Error> {
        match self.0 {
            ProcessHandleInner::Child {
                ref mut process,
                pipes_dropped: _,
            } => process.kill(),
            ProcessHandleInner::Pidfd {
                raw_pidfd,
                exited_rx: _,
                exited,
            } => {
                if exited.is_some() {
                    return Err(std::io::Error::other("Trying to send SIGKILL to exited process"));
                }

                crate::sys::pidfd_send_sigkill(raw_pidfd)
            }
        }
    }

    /// Wait for the process to have exited.
    pub async fn wait(&mut self) -> Result<ExitStatus, std::io::Error> {
        match self.0 {
            ProcessHandleInner::Child {
                ref mut process,
                pipes_dropped: _,
            } => process.wait().await,
            ProcessHandleInner::Pidfd {
                raw_pidfd: _,
                ref mut exited_rx,
                ref mut exited,
            } => {
                if let Some(exited) = exited {
                    return Ok(*exited);
                }

                let exit_status = exited_rx
                    .await
                    .map_err(|_| std::io::Error::other("Could not recv from task waiting on pidfd"))?;
                *exited = Some(exit_status);
                Ok(exit_status)
            }
        }
    }

    /// Check if the process has exited, returning the [ExitStatus] if so or [None] otherwise.
    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, std::io::Error> {
        match self.0 {
            ProcessHandleInner::Child {
                ref mut process,
                pipes_dropped: _,
            } => process.try_wait(),
            ProcessHandleInner::Pidfd {
                raw_pidfd: _,
                ref mut exited_rx,
                ref mut exited,
            } => {
                if let Some(exited) = exited {
                    return Ok(Some(*exited));
                }

                if let Ok(Some(exit_status)) = exited_rx.try_recv() {
                    *exited = Some(exit_status);
                    Ok(Some(exit_status))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Try to get the [ProcessHandlePipes] for this process. Only possible for attached (child)
    /// processes that haven't had their pipes dropped when creating.
    pub fn get_pipes(&mut self) -> Result<ProcessHandlePipes<P>, ProcessHandlePipesError> {
        match self.0 {
            ProcessHandleInner::Pidfd {
                raw_pidfd: _,
                exited_rx: _,
                exited: _,
            } => Err(ProcessHandlePipesError::ProcessIsDetached),
            ProcessHandleInner::Child {
                ref mut process,
                pipes_dropped,
            } => {
                if pipes_dropped {
                    return Err(ProcessHandlePipesError::PipesWereDropped);
                }

                let stdout = process
                    .take_stdout()
                    .ok_or(ProcessHandlePipesError::PipesWereAlreadyTaken)?;
                let stderr = process
                    .take_stderr()
                    .ok_or(ProcessHandlePipesError::PipesWereAlreadyTaken)?;
                let stdin = process
                    .take_stdin()
                    .ok_or(ProcessHandlePipesError::PipesWereAlreadyTaken)?;

                Ok(ProcessHandlePipes { stdout, stderr, stdin })
            }
        }
    }
}
