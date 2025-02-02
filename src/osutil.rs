//! Miscellaneous OS utilities

use std::process::Command;

/// Drops the OS page cache
pub fn drop_caches() {
    Command::new("sync").output().unwrap();
    let out = Command::new("sudo")
        .arg("/sbin/sysctl")
        .arg("vm.drop_caches=3")
        .output()
        .unwrap();
    if !out.status.success() {
        panic!(
            "Failed to drop caches: {}",
            std::str::from_utf8(&out.stderr).unwrap()
        );
    }
}

pub fn drop_path_from_cache(path: &str) {
    let out = Command::new("dd")
        .arg(format!("of={}", path))
        .arg("oflag=nocache")
        .arg("conv=notrunc,fdatasync")
        .arg("count=0")
        .output()
        .unwrap();
    if !out.status.success() {
        panic!(
            "Failed to drop caches: {}",
            std::str::from_utf8(&out.stderr).unwrap()
        );
    }
}
