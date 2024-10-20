//! Provides a wide variety of VMM-related APIs behind the following feature gates, in order of lower to higher level:
//! - `vmm-arguments`, full mappings to the CLI arguments of the "firecracker" and "jailer" binaries.
//! - `vmm-executor`, a low-level executor abstraction that manages a VMM environment and invokes it.
//! - `vmm-process`, a higher-level (but lower than a VM) abstraction that manages the VMM process's full functionality.

#[cfg(feature = "vmm-arguments")]
#[cfg_attr(docsrs, doc(cfg(feature = "vmm-arguments")))]
pub mod arguments;

#[cfg(feature = "vmm-arguments")]
#[cfg_attr(docsrs, doc(cfg(feature = "vmm-arguments")))]
pub mod id;

#[cfg(feature = "vmm-executor")]
#[cfg_attr(docsrs, doc(cfg(feature = "vmm-executor")))]
pub mod executor;

#[cfg(feature = "vmm-installation")]
#[cfg_attr(docsrs, doc(cfg(feature = "vmm-installation")))]
pub mod installation;

#[cfg(feature = "vmm-process")]
#[cfg_attr(docsrs, doc(cfg(feature = "vmm-process")))]
pub mod process;
