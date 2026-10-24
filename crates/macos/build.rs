use std::{env, path::PathBuf, process::Command};

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../platform/macos")
        .canonicalize()
        .unwrap();
    println!("cargo:rerun-if-changed={}", root.join("Sources").display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Package.swift").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Info.plist").display()
    );
    let header = root.join("Sources/CLiazoCapture/include/LiazoCapture.h");
    bindgen::Builder::default()
        .header(header.to_str().unwrap())
        .allowlist_function("liazo_native_.*")
        .allowlist_type("Liazo.*")
        .derive_default(true)
        .layout_tests(false)
        .generate_comments(false)
        .generate()
        .expect("generate native capture bindings")
        .write_to_file(PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("capture.rs"))
        .unwrap();
    let status = Command::new("swift")
        .args(["build", "--package-path"])
        .arg(&root)
        .args(["--configuration", "release", "--product", "LiazoCapture"])
        .status()
        .expect("run swift build");
    assert!(status.success(), "native capture build failed");
    let output = Command::new("swift")
        .args(["build", "--package-path"])
        .arg(&root)
        .args(["--configuration", "release", "--show-bin-path"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let library = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-link-search=native={}", library.trim());
    let sdk = Command::new("xcrun")
        .arg("--show-sdk-path")
        .output()
        .unwrap();
    let sdk = String::from_utf8(sdk.stdout).unwrap();
    println!(
        "cargo:rustc-link-search=native={}/usr/lib/swift",
        sdk.trim()
    );
    println!("cargo:rustc-link-search=native=/usr/lib/swift");
    println!("cargo:rustc-link-lib=static=LiazoCapture");
    println!("cargo:rustc-link-lib=dylib=swiftCore");
    println!("cargo:rustc-link-lib=dylib=swift_Concurrency");
    println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/swift");
    println!(
        "cargo:rustc-link-arg=-Wl,-sectcreate,__TEXT,__info_plist,{}",
        root.join("Info.plist").display()
    );
    println!("cargo:swift_rpath=/usr/lib/swift");
    println!("cargo:info_plist={}", root.join("Info.plist").display());
    for framework in [
        "Foundation",
        "AppKit",
        "CoreGraphics",
        "CoreAudio",
        "AVFoundation",
        "AudioToolbox",
        "ScreenCaptureKit",
        "ImageIO",
        "UniformTypeIdentifiers",
        "CryptoKit",
        "CoreFoundation",
    ] {
        println!("cargo:rustc-link-lib=framework={framework}");
    }
}
