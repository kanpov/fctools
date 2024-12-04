#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(any(
    feature = "vmm-arguments",
    feature = "vmm-installation",
    feature = "vmm-process",
    feature = "vmm-executor",
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "vmm-arguments",
        feature = "vmm-installation",
        feature = "vmm-process",
        feature = "vmm-executor",
    )))
)]
pub mod vmm;

#[cfg(any(
    feature = "grpc-vsock-extension",
    feature = "http-vsock-extension",
    feature = "link-local-extension",
    feature = "metrics-extension",
    feature = "snapshot-editor-extension",
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "grpc-vsock-extension",
        feature = "http-vsock-extension",
        feature = "link-local-extension",
        feature = "metrics-extension",
        feature = "snapshot-editor-extension",
    )))
)]
pub mod extension;

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
pub mod runtime;

#[cfg(feature = "process-spawner")]
#[cfg_attr(docsrs, doc(cfg(feature = "process-spawner")))]
pub mod process_spawner;

#[cfg(feature = "vm")]
#[cfg_attr(docsrs, doc(cfg(feature = "vm")))]
pub mod vm;

#[cfg(feature = "runtime")]
pub(crate) mod sys;
