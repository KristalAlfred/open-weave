use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for artifact in weave_core::contracts::artifacts() {
        let path = root.join(artifact.path);
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(path, artifact.content)?;
    }
    Ok(())
}
