//! Build script to compile protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the protobuf definitions.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile(&["proto/helix.proto"], &["proto"])?;

    // Tell Cargo to rerun if the proto file changes.
    println!("cargo:rerun-if-changed=proto/helix.proto");

    Ok(())
}
