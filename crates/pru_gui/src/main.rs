mod app;

use app::PruGuiApp;

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "PRU-DB Explorer",
        native_options,
        Box::new(|_cc| Box::new(PruGuiApp::default())),
    )
}
