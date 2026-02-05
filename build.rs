use std::process::Command;

fn main() {
    let hash = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=GIT_HASH={}", hash);
    println!("cargo:rustc-env=GIT_BRANCH={}", branch);

    // Para el updater: si no están, usamos fallbacks locales
    let git_sha = std::env::var("GIT_SHA").unwrap_or(hash);
    let build_ts = std::env::var("BUILD_TS").unwrap_or_else(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    });
    println!("cargo:rustc-env=GIT_SHA={}", git_sha);
    println!("cargo:rustc-env=BUILD_TS={}", build_ts);
}
