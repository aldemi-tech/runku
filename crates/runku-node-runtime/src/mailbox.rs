use std::{net::SocketAddr, path::Path, time::Duration};

use runku_core::InvocationId;
use runku_runtime::{CancellationToken, RuntimeError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const READY_FILE: &str = "ready";
const REQUESTS_DIRECTORY: &str = "requests";
const RESPONSES_DIRECTORY: &str = "responses";
const MAX_HANDSHAKE_BYTES: usize = 256;

pub(crate) struct TcpMailbox {
    stream: TcpStream,
}

impl TcpMailbox {
    pub(crate) async fn connect(
        address: SocketAddr,
        token: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Self, RuntimeError> {
        if token.is_empty() || token.len() > MAX_HANDSHAKE_BYTES {
            return Err(RuntimeError::InvalidConfiguration);
        }
        loop {
            match TcpStream::connect(address).await {
                Ok(mut stream) => {
                    stream
                        .set_nodelay(true)
                        .map_err(|_| RuntimeError::Unavailable)?;
                    write_frame(&mut stream, token.as_bytes(), deadline, None).await?;
                    let response =
                        read_frame(&mut stream, MAX_HANDSHAKE_BYTES, deadline, None).await?;
                    if response != b"READY" {
                        return Err(RuntimeError::Unavailable);
                    }
                    return Ok(Self { stream });
                }
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(_) => return Err(RuntimeError::Unavailable),
            }
        }
    }

    pub(crate) async fn invoke(
        &mut self,
        input: &[u8],
        max_output_bytes: usize,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, RuntimeError> {
        write_frame(&mut self.stream, input, deadline, Some(cancellation)).await?;
        read_frame(
            &mut self.stream,
            max_output_bytes,
            deadline,
            Some(cancellation),
        )
        .await
    }
}

async fn write_frame(
    stream: &mut TcpStream,
    bytes: &[u8],
    deadline: tokio::time::Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<(), RuntimeError> {
    let length = u32::try_from(bytes.len()).map_err(|_| RuntimeError::InvalidArguments)?;
    let write = async {
        stream
            .write_all(&length.to_be_bytes())
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        stream
            .write_all(bytes)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        stream.flush().await.map_err(|_| RuntimeError::Unavailable)
    };
    if let Some(cancellation) = cancellation {
        tokio::select! {
            result = write => result,
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            () = tokio::time::sleep_until(deadline) => Err(RuntimeError::DeadlineExceeded),
        }
    } else {
        tokio::select! {
            result = write => result,
            () = tokio::time::sleep_until(deadline) => Err(RuntimeError::DeadlineExceeded),
        }
    }
}

async fn read_frame(
    stream: &mut TcpStream,
    max_bytes: usize,
    deadline: tokio::time::Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, RuntimeError> {
    let read = async {
        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        let length =
            usize::try_from(u32::from_be_bytes(header)).map_err(|_| RuntimeError::InvalidResult)?;
        if length == 0 || length > max_bytes {
            return Err(RuntimeError::InvalidResult);
        }
        let mut bytes = vec![0_u8; length];
        stream
            .read_exact(&mut bytes)
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        Ok(bytes)
    };
    if let Some(cancellation) = cancellation {
        tokio::select! {
            result = read => result,
            () = cancellation.cancelled() => Err(RuntimeError::Cancelled),
            () = tokio::time::sleep_until(deadline) => Err(RuntimeError::DeadlineExceeded),
        }
    } else {
        tokio::select! {
            result = read => result,
            () = tokio::time::sleep_until(deadline) => Err(RuntimeError::DeadlineExceeded),
        }
    }
}

pub(crate) async fn prepare(root: &Path) -> Result<(), RuntimeError> {
    tokio::fs::create_dir(root)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    tokio::fs::create_dir(root.join(REQUESTS_DIRECTORY))
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    tokio::fs::create_dir(root.join(RESPONSES_DIRECTORY))
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    make_shared(root)?;
    make_shared(&root.join(REQUESTS_DIRECTORY))?;
    make_shared(&root.join(RESPONSES_DIRECTORY))
}

#[cfg(unix)]
fn make_shared(path: &Path) -> Result<(), RuntimeError> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
        .map_err(|_| RuntimeError::Unavailable)
}

#[cfg(not(unix))]
fn make_shared(_path: &Path) -> Result<(), RuntimeError> {
    Ok(())
}

pub(crate) async fn wait_ready(
    root: &Path,
    deadline: tokio::time::Instant,
) -> Result<(), RuntimeError> {
    let ready = root.join(READY_FILE);
    loop {
        match tokio::fs::symlink_metadata(&ready).await {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(RuntimeError::Unavailable),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RuntimeError::Unavailable);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

pub(crate) async fn invoke(
    root: &Path,
    invocation_id: InvocationId,
    input: &[u8],
    max_output_bytes: usize,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, RuntimeError> {
    let name = invocation_id.to_string();
    let request = root.join(REQUESTS_DIRECTORY).join(format!("{name}.json"));
    let request_staging = root
        .join(REQUESTS_DIRECTORY)
        .join(format!(".{name}.staging"));
    let response = root.join(RESPONSES_DIRECTORY).join(format!("{name}.json"));
    write_atomic(&request_staging, &request, input).await?;

    let result = wait_response(&response, max_output_bytes, deadline, cancellation).await;
    remove_if_present(&request_staging).await;
    remove_if_present(&request).await;
    remove_if_present(&response).await;
    result
}

async fn write_atomic(staging: &Path, final_path: &Path, bytes: &[u8]) -> Result<(), RuntimeError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options
        .open(staging)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    file.write_all(bytes)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    file.shutdown()
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    tokio::fs::rename(staging, final_path)
        .await
        .map_err(|_| RuntimeError::Unavailable)
}

async fn wait_response(
    response: &Path,
    max_output_bytes: usize,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, RuntimeError> {
    loop {
        let read = read_response(response, max_output_bytes).await;
        match read {
            Ok(Some(bytes)) => return Ok(bytes),
            Ok(None) => {}
            Err(error) => return Err(error),
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(RuntimeError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(RuntimeError::DeadlineExceeded);
            }
            () = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
    }
}

async fn read_response(
    response: &Path,
    max_output_bytes: usize,
) -> Result<Option<Vec<u8>>, RuntimeError> {
    let metadata = match tokio::fs::symlink_metadata(response).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(RuntimeError::Unavailable),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RuntimeError::InvalidResult);
    }
    if metadata.len() > u64::try_from(max_output_bytes).map_err(|_| RuntimeError::Internal)? {
        return Err(RuntimeError::InvalidResult);
    }
    let bytes = tokio::fs::read(response)
        .await
        .map_err(|_| RuntimeError::Unavailable)?;
    if bytes.len() > max_output_bytes {
        return Err(RuntimeError::InvalidResult);
    }
    Ok(Some(bytes))
}

async fn remove_if_present(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}
