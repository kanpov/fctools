use std::path::PathBuf;

use assert_matches::assert_matches;
use fctools::executor::{
    arguments::{ConfigurationFileOverride, JailerArguments, VmmApiSocket, VmmArguments},
    command_modifier::{AppendCommandModifier, RewriteCommandModifier},
    jailed::{FlatJailRenamer, JailMoveMethod, JailRenamer, JailedVmmExecutor},
    VmmExecutor, VmmExecutorError,
};
use rand::RngCore;
use test_framework::{get_fake_firecracker_installation, get_fs_backend, get_shell_spawner, get_tmp_path, jail_join};
use tokio::fs::{create_dir_all, metadata, read_to_string, remove_dir_all, try_exists, write, File};

mod test_framework;

#[test]
fn jailed_executor_get_socket_path_returns_valid_path() {
    assert_eq!(
        setup_executor(None, None, None, None, None)
            .0
            .get_socket_path(&get_fake_firecracker_installation()),
        None
    );

    let path = get_tmp_path();
    let (executor, jail_path) = setup_executor(None, Some(path.clone()), None, None, None);
    assert_eq!(
        executor.get_socket_path(&get_fake_firecracker_installation()),
        Some(jail_join(jail_path, path))
    );
}

#[test]
fn jailed_executor_inner_to_outer_path_should_perform_transformation() {
    let (executor, jail_path) = setup_executor(None, None, None, None, None);
    for path in ["/a", "/b/c", "/d/e/f", "/g/h/i/j"].iter().map(PathBuf::from) {
        assert_eq!(
            executor.inner_to_outer_path(&get_fake_firecracker_installation(), &path),
            jail_join(&jail_path, path)
        );
    }
}

#[tokio::test]
async fn jailed_executor_prepare_creates_chroot_base_dir() {
    let chroot_base_dir = get_tmp_path();
    let (executor, _) = setup_executor(Some(chroot_base_dir.clone()), None, None, None, None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(metadata(&chroot_base_dir).await.unwrap().is_dir());
    remove_dir_all(chroot_base_dir).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_defaults_to_srv_jailer() {
    let (executor, jail_path) = setup_executor(None, None, None, None, None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(metadata(&jail_path).await.unwrap().is_dir());
    remove_dir_all(&jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_deletes_existing_jail() {
    let (executor, jail_path) = setup_executor(None, None, None, None, None);
    create_dir_all(&jail_path).await.unwrap();
    let jail_inner_path = jail_path.join("some_file.txt");
    File::create_new(&jail_inner_path).await.unwrap();

    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(!try_exists(jail_inner_path).await.unwrap());
    assert!(try_exists(&jail_path).await.unwrap());
    remove_dir_all(jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_creates_socket_parent_directory() {
    let (executor, jail_path) = setup_executor(None, Some(PathBuf::from("/parent_dir/socket")), None, None, None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(try_exists(jail_path.join("parent_dir")).await.unwrap());
    remove_dir_all(jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_creates_log_file() {
    let (executor, jail_path) = setup_executor(None, None, Some(PathBuf::from("/log.txt")), None, None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(try_exists(jail_path.join("log.txt")).await.unwrap());
    remove_dir_all(jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_creates_metrics_file() {
    let (executor, jail_path) = setup_executor(None, None, None, Some(PathBuf::from("/metrics.txt")), None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(try_exists(jail_path.join("metrics.txt")).await.unwrap());
    remove_dir_all(jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_checks_for_missing_resources() {
    let (executor, jail_path) = setup_executor(None, None, None, None, None);
    let resource_path = get_tmp_path();
    assert_matches!(
        executor
            .prepare(
                &get_fake_firecracker_installation(),
                get_shell_spawner(),
                get_fs_backend(),
                vec![resource_path.clone()]
            )
            .await,
        Err(VmmExecutorError::ExpectedResourceMissing(_))
    );
    remove_dir_all(jail_path).await.unwrap();
}

#[tokio::test]
async fn jailed_executor_prepare_moves_resources_by_copy() {
    move_test_imp(JailMoveMethod::Copy, false).await;
}

#[tokio::test]
async fn jailed_executor_prepare_moves_resources_by_hard_link() {
    move_test_imp(JailMoveMethod::HardLink, false).await;
}

#[tokio::test]
async fn jailed_executor_prepare_moves_resources_by_hard_link_with_copy_fallback() {
    move_test_imp(JailMoveMethod::HardLinkWithCopyFallback, true).await;
}

#[tokio::test]
async fn jailed_executor_invoke_applies_command_modifier_chain() {
    let (mut executor, _) = setup_executor(None, None, None, None, None);
    executor = executor
        .command_modifier(RewriteCommandModifier::new("echo"))
        .command_modifier(AppendCommandModifier::new(" test"));
    let child = executor
        .invoke(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            ConfigurationFileOverride::NoOverride,
        )
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(child.wait_with_output().await.unwrap().stdout).unwrap(),
        "test\n"
    );
}

#[tokio::test]
async fn jailed_executor_cleanup_recursively_removes_entire_jail() {
    let (executor, jail_path) = setup_executor(None, None, None, None, None);
    executor
        .prepare(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
            vec![],
        )
        .await
        .unwrap();
    assert!(try_exists(&jail_path).await.unwrap());
    executor
        .cleanup(
            &get_fake_firecracker_installation(),
            get_shell_spawner(),
            get_fs_backend(),
        )
        .await
        .unwrap();
    assert!(!try_exists(jail_path).await.unwrap());
}

async fn move_test_imp(jail_move_method: JailMoveMethod, try_cross_device: bool) {
    let chroot_base_dir = match try_cross_device {
        true => PathBuf::from("/srv/jailer"),
        false => get_tmp_path(),
    };
    let (executor, jail_path) = setup_executor(Some(chroot_base_dir.clone()), None, None, None, Some(jail_move_method));
    let resource_path = get_tmp_path();
    let resource_inner_path = FlatJailRenamer::default().rename_for_jail(&resource_path).unwrap();
    write(&resource_path, b"test").await.unwrap();

    assert_eq!(
        executor
            .prepare(
                &get_fake_firecracker_installation(),
                get_shell_spawner(),
                get_fs_backend(),
                vec![resource_path.clone()]
            )
            .await
            .unwrap()
            .get(&resource_path),
        Some(&resource_inner_path)
    );
    assert_eq!(
        read_to_string(jail_join(&jail_path, resource_inner_path))
            .await
            .unwrap(),
        "test"
    );

    remove_dir_all(jail_path).await.unwrap();
}

fn setup_executor(
    chroot_base_dir: Option<PathBuf>,
    socket_path: Option<PathBuf>,
    log_path: Option<PathBuf>,
    metrics_path: Option<PathBuf>,
    jail_move_method: Option<JailMoveMethod>,
) -> (JailedVmmExecutor<FlatJailRenamer>, PathBuf) {
    let jail_id = rand::thread_rng().next_u32().to_string();
    let actual_chroot_base_dir = chroot_base_dir.clone().or(Some(PathBuf::from("/srv/jailer"))).unwrap();
    let mut jailer_arguments = JailerArguments::new(unsafe { libc::geteuid() }, unsafe { libc::getegid() }, &jail_id);
    if let Some(chroot_base_dir) = chroot_base_dir {
        jailer_arguments = jailer_arguments.chroot_base_dir(chroot_base_dir);
    }
    let mut firecracker_arguments =
        VmmArguments::new(socket_path.map_or(VmmApiSocket::Disabled, VmmApiSocket::Enabled));
    if let Some(log_path) = log_path {
        firecracker_arguments = firecracker_arguments.log_path(log_path);
    }
    if let Some(metrics_path) = metrics_path {
        firecracker_arguments = firecracker_arguments.metrics_path(metrics_path);
    }

    let executor = JailedVmmExecutor::new(firecracker_arguments, jailer_arguments, FlatJailRenamer::default())
        .jail_move_method(jail_move_method.or(Some(JailMoveMethod::Copy)).unwrap());

    let jail_path = actual_chroot_base_dir.join("firecracker").join(jail_id).join("root");
    (executor, jail_path)
}
