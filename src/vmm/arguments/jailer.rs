use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

/// Arguments that are passed by relevant executors into the "jailer" binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JailerArguments {
    uid: u32,
    gid: u32,
    pub(crate) jail_id: String,

    cgroup_values: HashMap<String, String>,
    cgroup_version: Option<JailerCgroupVersion>,
    pub(crate) chroot_base_dir: Option<PathBuf>,
    daemonize: bool,
    network_namespace_path: Option<PathBuf>,
    exec_in_new_pid_ns: bool,
    parent_cgroup: Option<String>,
    resource_limits: HashMap<String, String>,
}

impl JailerArguments {
    pub fn new(uid: u32, gid: u32, jail_id: impl Into<String>) -> Self {
        Self {
            uid,
            gid,
            jail_id: jail_id.into(),
            cgroup_values: HashMap::new(),
            cgroup_version: None,
            chroot_base_dir: None,
            daemonize: false,
            network_namespace_path: None,
            exec_in_new_pid_ns: false,
            parent_cgroup: None,
            resource_limits: HashMap::new(),
        }
    }

    pub fn cgroup(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.cgroup_values.insert(key.into(), value.into());
        self
    }

    pub fn cgroups(mut self, cgroups: impl IntoIterator<Item = (String, String)>) -> Self {
        self.cgroup_values.extend(cgroups);
        self
    }

    pub fn cgroup_version(mut self, cgroup_version: JailerCgroupVersion) -> Self {
        self.cgroup_version = Some(cgroup_version);
        self
    }

    pub fn chroot_base_dir(mut self, chroot_base_dir: impl Into<PathBuf>) -> Self {
        self.chroot_base_dir = Some(chroot_base_dir.into());
        self
    }

    pub fn daemonize(mut self) -> Self {
        self.daemonize = true;
        self
    }

    pub fn network_namespace_path(mut self, network_namespace_path: impl Into<PathBuf>) -> Self {
        self.network_namespace_path = Some(network_namespace_path.into());
        self
    }

    pub fn exec_in_new_pid_ns(mut self) -> Self {
        self.exec_in_new_pid_ns = true;
        self
    }

    pub fn parent_cgroup(mut self, parent_cgroup: impl Into<String>) -> Self {
        self.parent_cgroup = Some(parent_cgroup.into());
        self
    }

    pub fn resource_limit(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resource_limits.insert(key.into(), value.into());
        self
    }

    pub fn resource_limits(mut self, resource_limits: impl IntoIterator<Item = (String, String)>) -> Self {
        self.resource_limits.extend(resource_limits);
        self
    }

    pub fn join(&self, firecracker_binary_path: &Path) -> Vec<String> {
        let mut args = Vec::with_capacity(8);
        args.push("--exec-file".to_string());
        args.push(firecracker_binary_path.to_string_lossy().into_owned());
        args.push("--uid".to_string());
        args.push(self.uid.to_string());
        args.push("--gid".to_string());
        args.push(self.gid.to_string());
        args.push("--id".to_string());
        args.push(self.jail_id.to_string());

        if !self.cgroup_values.is_empty() {
            for (key, value) in &self.cgroup_values {
                args.push("--cgroup".to_string());
                args.push(format!("{key}={value}"));
            }
        }

        if let Some(cgroup_version) = self.cgroup_version {
            args.push("--cgroup-version".to_string());
            args.push(match cgroup_version {
                JailerCgroupVersion::V1 => "1".to_string(),
                JailerCgroupVersion::V2 => "2".to_string(),
            });
        }

        if let Some(ref chroot_base_dir) = self.chroot_base_dir {
            args.push("--chroot-base-dir".to_string());
            args.push(chroot_base_dir.to_string_lossy().into_owned());
        }

        if self.daemonize {
            args.push("--daemonize".to_string());
        }

        if let Some(ref network_namespace_path) = self.network_namespace_path {
            args.push("--netns".to_string());
            args.push(network_namespace_path.to_string_lossy().into_owned());
        }

        if self.exec_in_new_pid_ns {
            args.push("--new-pid-ns".to_string());
        }

        if let Some(parent_cgroup) = self.parent_cgroup.clone() {
            args.push("--parent-cgroup".to_string());
            args.push(parent_cgroup);
        }

        if !self.resource_limits.is_empty() {
            for (key, value) in &self.resource_limits {
                args.push("--resource-limit".to_string());
                args.push(format!("{key}={value}"));
            }
        }

        args
    }
}

/// The cgroup version used by the jailer, v1 by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JailerCgroupVersion {
    /// Cgroups v1
    V1,
    /// Cgroups v2
    V2,
}
