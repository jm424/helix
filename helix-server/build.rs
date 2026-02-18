//! Build script to compile protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    // Compile the protobuf definitions.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile(&["proto/helix.proto"], &["proto"])?;

    // Compile the admin proto (kafkaadmin.Resources service).
    // Client enabled for leader forwarding (non-leader nodes proxy to the leader).
    // File descriptor set emitted for gRPC reflection.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .file_descriptor_set_path(out_dir.join("admin_descriptor.bin"))
        .compile(&["proto/admin.proto"], &["proto"])?;

    // Tell Cargo to rerun if proto files change.
    println!("cargo:rerun-if-changed=proto/helix.proto");
    println!("cargo:rerun-if-changed=proto/admin.proto");

    Ok(())
}
