#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod bridge;

fn main() {
    tauri::Builder::default()
        .manage(bridge::Bridge::new().expect("local client setup failed"))
        .invoke_handler(tauri::generate_handler![
            bridge::operation,
            bridge::media,
            bridge::release_media
        ])
        .run(tauri::generate_context!())
        .expect("desktop application failed");
}
