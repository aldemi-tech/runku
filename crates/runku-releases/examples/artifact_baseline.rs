//! Reproducible local SHA-256/write/read+verify artifact baseline.

use std::{error::Error, hint::black_box, time::Instant};

use runku_releases::{
    ArtifactDescriptor, ArtifactFormat, ArtifactStore, FilesystemArtifactStore,
    FilesystemStoreRole, Sha256Digest,
};
use tempfile::tempdir;

const ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
const HASH_ITERATIONS: u32 = 20;
const READ_ITERATIONS: u32 = 20;

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let store = FilesystemArtifactStore::open(directory.path(), FilesystemStoreRole::Test).await?;
    let mut artifact = vec![0_u8; ARTIFACT_BYTES];
    for (index, byte) in artifact.iter_mut().enumerate() {
        *byte = u8::try_from(index % 251)?;
    }

    let hash_started = Instant::now();
    let mut digest = Sha256Digest::of(&artifact);
    for _ in 1..HASH_ITERATIONS {
        digest = black_box(Sha256Digest::of(black_box(&artifact)));
    }
    let hash_micros = hash_started.elapsed().as_micros();

    let descriptor = ArtifactDescriptor {
        format: ArtifactFormat::SafeEsmBundleV1,
        digest,
        size_bytes: u64::try_from(artifact.len())?,
    };

    let write_started = Instant::now();
    store.put(&descriptor, &artifact).await?;
    let write_micros = write_started.elapsed().as_micros();

    let read_started = Instant::now();
    for _ in 0..READ_ITERATIONS {
        let loaded = black_box(store.get(&descriptor).await?);
        if loaded.len() != ARTIFACT_BYTES {
            return Err("artifact read length changed".into());
        }
    }
    let read_micros = read_started.elapsed().as_micros();

    println!("artifact_bytes={ARTIFACT_BYTES}");
    println!("hash_iterations={HASH_ITERATIONS}");
    println!("hash_total_micros={hash_micros}");
    println!("atomic_put_micros={write_micros}");
    println!("read_iterations={READ_ITERATIONS}");
    println!("read_verify_total_micros={read_micros}");
    println!("digest={digest}");
    Ok(())
}
