use std::path::PathBuf;

pub mod command_modifier;
pub mod jailer;

/// Arguments that can be passed to the main VMM/"firecracker" binary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VmmArguments {
    // main
    pub(crate) api_socket: VmmApiSocket,
    config_path: Option<PathBuf>,
    // logging
    log_level: Option<VmmLogLevel>,
    pub(crate) log_path: Option<PathBuf>,
    show_log_origin: bool,
    log_module: Option<String>,
    show_log_level: bool,
    // misc
    enable_boot_timer: bool,
    api_max_payload_bytes: Option<u32>,
    metadata_path: Option<PathBuf>,
    pub(crate) metrics_path: Option<PathBuf>,
    mmds_size_limit: Option<u32>,
    disable_seccomp: bool,
    seccomp_path: Option<PathBuf>,
}

/// An override that can be applied to the configuration file passed down to Firecracker. This
/// exists in order to allow upstream overrides to work, which are necessary on the VM layer that
/// would create and enforce a configuration file with an init method.
pub enum VmmConfigurationOverride {
    NoOverride,
    Disable,
    Enable(PathBuf),
}

impl VmmArguments {
    pub fn new(api_socket: VmmApiSocket) -> Self {
        Self {
            api_socket,
            config_path: None,
            log_level: None,
            log_path: None,
            show_log_origin: false,
            log_module: None,
            show_log_level: false,
            enable_boot_timer: false,
            api_max_payload_bytes: None,
            metadata_path: None,
            metrics_path: None,
            mmds_size_limit: None,
            disable_seccomp: false,
            seccomp_path: None,
        }
    }

    pub fn config_path(mut self, config_path: impl Into<PathBuf>) -> Self {
        self.config_path = Some(config_path.into());
        self
    }

    pub fn log_level(mut self, log_level: VmmLogLevel) -> Self {
        self.log_level = Some(log_level);
        self
    }

    pub fn log_path(mut self, log_path: impl Into<PathBuf>) -> Self {
        self.log_path = Some(log_path.into());
        self
    }

    pub fn show_log_level(mut self) -> Self {
        self.show_log_level = true;
        self
    }

    pub fn show_log_origin(mut self) -> Self {
        self.show_log_origin = true;
        self
    }

    pub fn log_module(mut self, log_module: impl Into<String>) -> Self {
        self.log_module = Some(log_module.into());
        self
    }

    pub fn enable_boot_timer(mut self) -> Self {
        self.enable_boot_timer = true;
        self
    }

    pub fn api_max_payload_bytes(mut self, amount: u32) -> Self {
        self.api_max_payload_bytes = Some(amount);
        self
    }

    pub fn metadata_path(mut self, metadata_path: impl Into<PathBuf>) -> Self {
        self.metadata_path = Some(metadata_path.into());
        self
    }

    pub fn metrics_path(mut self, metrics_path: impl Into<PathBuf>) -> Self {
        self.metrics_path = Some(metrics_path.into());
        self
    }

    pub fn mmds_size_limit(mut self, mmds_size_limit: u32) -> Self {
        self.mmds_size_limit = Some(mmds_size_limit);
        self
    }

    pub fn disable_seccomp(mut self) -> Self {
        self.disable_seccomp = true;
        self
    }

    pub fn seccomp_path(mut self, seccomp_path: impl Into<PathBuf>) -> Self {
        self.seccomp_path = Some(seccomp_path.into());
        self
    }

    pub fn join(&self, config_override: VmmConfigurationOverride) -> Vec<String> {
        let mut args = Vec::with_capacity(1);

        match self.api_socket {
            VmmApiSocket::Disabled => {
                args.push("--no-api".to_string());
            }
            VmmApiSocket::Enabled(ref socket_path) => {
                args.push("--api-sock".to_string());
                args.push(socket_path.to_string_lossy().into_owned());
            }
        }

        match config_override {
            VmmConfigurationOverride::NoOverride => {
                if let Some(ref config_path) = self.config_path {
                    args.push("--config-file".to_string());
                    args.push(config_path.to_string_lossy().into_owned());
                }
            }
            VmmConfigurationOverride::Disable => {}
            VmmConfigurationOverride::Enable(path) => {
                args.push("--config-file".to_string());
                args.push(path.to_string_lossy().into_owned());
            }
        }

        if let Some(log_level) = self.log_level {
            args.push("--level".to_string());
            args.push(log_level.to_string());
        }

        if let Some(ref log_path) = self.log_path {
            args.push("--log-path".to_string());
            args.push(log_path.to_string_lossy().into_owned());
        }

        if self.show_log_origin {
            args.push("--show-log-origin".to_string());
        }

        if let Some(module) = self.log_module.clone() {
            args.push("--module".to_string());
            args.push(module);
        }

        if self.show_log_level {
            args.push("--show-level".to_string());
        }

        if self.enable_boot_timer {
            args.push("--boot-timer".to_string());
        }

        if let Some(max_payload) = self.api_max_payload_bytes {
            args.push("--http-api-max-payload-size".to_string());
            args.push(max_payload.to_string());
        }

        if let Some(ref metadata_path) = self.metadata_path {
            args.push("--metadata".to_string());
            args.push(metadata_path.to_string_lossy().into_owned());
        }

        if let Some(ref metrics_path) = self.metrics_path {
            args.push("--metrics-path".to_string());
            args.push(metrics_path.to_string_lossy().into_owned());
        }

        if let Some(ref limit) = self.mmds_size_limit {
            args.push("--mmds-size-limit".to_string());
            args.push(limit.to_string());
        }

        if self.disable_seccomp {
            args.push("--no-seccomp".to_string());
        }

        if let Some(ref seccomp_path) = self.seccomp_path {
            args.push("--seccomp-filter".to_string());
            args.push(seccomp_path.to_string_lossy().into_owned());
        }

        args
    }
}

/// A configuration of a VMM API Unix socket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum VmmApiSocket {
    /// The socket should be disabled via --no-api argument.
    Disabled,
    /// The socket should be enabled at the given path via --api-sock argument.
    Enabled(PathBuf),
}

/// A level of logging applied by the VMM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "vm", derive(serde::Serialize, serde::Deserialize))]
pub enum VmmLogLevel {
    Off,
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl ToString for VmmLogLevel {
    fn to_string(&self) -> String {
        match self {
            VmmLogLevel::Off => "Off",
            VmmLogLevel::Trace => "Trace",
            VmmLogLevel::Debug => "Debug",
            VmmLogLevel::Info => "Info",
            VmmLogLevel::Warn => "Warn",
            VmmLogLevel::Error => "Error",
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{VmmApiSocket, VmmArguments, VmmConfigurationOverride, VmmLogLevel};

    fn new() -> VmmArguments {
        VmmArguments::new(VmmApiSocket::Enabled(PathBuf::from("/tmp/api.sock")))
    }

    #[test]
    fn api_sock_can_be_disabled() {
        check_without_override(VmmArguments::new(VmmApiSocket::Disabled), ["--no-api"]);
    }

    #[test]
    fn api_sock_can_be_enabled() {
        check_without_override(new(), ["--api-sock", "/tmp/api.sock"]);
    }

    #[test]
    fn log_level_can_be_set() {
        check_without_override(new().log_level(VmmLogLevel::Error), ["--level", "Error"]);
    }

    #[test]
    fn log_path_can_be_set() {
        check_without_override(
            new().log_path("/tmp/some_logs.txt"),
            ["--log-path", "/tmp/some_logs.txt"],
        );
    }

    #[test]
    fn show_log_origin_can_be_enabled() {
        check_without_override(new().show_log_origin(), ["--show-log-origin"]);
    }

    #[test]
    fn module_can_be_set() {
        check_without_override(new().log_module("some_module"), ["--module", "some_module"]);
    }

    #[test]
    fn show_log_level_can_be_enabled() {
        check_without_override(new().show_log_level(), ["--show-level"]);
    }

    #[test]
    fn boot_timer_can_be_enabled() {
        check_without_override(new().enable_boot_timer(), ["--boot-timer"]);
    }

    #[test]
    fn max_payload_can_be_set() {
        check_without_override(
            new().api_max_payload_bytes(1000),
            ["--http-api-max-payload-size", "1000"],
        );
    }

    #[test]
    fn metadata_path_can_be_set() {
        check_without_override(
            new().metadata_path("/tmp/metadata.txt"),
            ["--metadata", "/tmp/metadata.txt"],
        );
    }

    #[test]
    fn metrics_path_can_be_set() {
        check_without_override(
            new().metrics_path("/tmp/metrics.txt"),
            ["--metrics-path", "/tmp/metrics.txt"],
        );
    }

    #[test]
    fn mmds_size_limit_can_be_set() {
        check_without_override(new().mmds_size_limit(1000), ["--mmds-size-limit", "1000"]);
    }

    #[test]
    fn seccomp_can_be_disabled() {
        check_without_override(new().disable_seccomp(), ["--no-seccomp"]);
    }

    #[test]
    fn seccomp_path_can_be_set() {
        check_without_override(new().seccomp_path("/tmp/seccomp"), ["--seccomp-filter", "/tmp/seccomp"]);
    }

    #[test]
    fn config_path_persists_without_override() {
        check_with_override(
            new().config_path("/tmp/config.json"),
            VmmConfigurationOverride::NoOverride,
            ["--config-file", "/tmp/config.json"],
        );
    }

    #[test]
    fn config_path_gets_added_with_enable_override() {
        for args in [new(), new().config_path("/tmp/config.json")] {
            check_with_override(
                args,
                VmmConfigurationOverride::Enable(PathBuf::from("/tmp/override_config.json")),
                ["--config-file", "/tmp/override_config.json"],
            );
        }
    }

    #[test]
    fn config_path_gets_removed_with_disable_override() {
        check_with_override(
            new().config_path("/tmp/config.json"),
            VmmConfigurationOverride::Disable,
            ["!--config-file", "!/tmp/config.json"],
        );
    }

    #[inline]
    fn check_without_override<const AMOUNT: usize>(args: VmmArguments, matchers: [&str; AMOUNT]) {
        check_with_override(args, VmmConfigurationOverride::NoOverride, matchers);
    }

    #[inline]
    fn check_with_override<const AMOUNT: usize>(
        args: VmmArguments,
        config_override: VmmConfigurationOverride,
        matchers: [&str; AMOUNT],
    ) {
        let joined_args = args.join(config_override);

        for matcher in matchers {
            if let Some(matcher) = matcher.strip_prefix("!") {
                assert!(!joined_args.contains(&matcher.to_string()));
            } else {
                assert!(joined_args.contains(&matcher.to_string()));
            }
        }
    }
}
