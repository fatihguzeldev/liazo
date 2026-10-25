fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        if let Ok(path) = std::env::var("DEP_LIAZO_CAPTURE_SWIFT_RPATH") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
        }
        if let Ok(path) = std::env::var("DEP_LIAZO_CAPTURE_INFO_PLIST") {
            println!("cargo:rustc-link-arg=-Wl,-sectcreate,__TEXT,__info_plist,{path}");
        }
    }
}
