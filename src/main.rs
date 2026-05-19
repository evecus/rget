use anyhow::{Context, Result};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder};
use clap::Parser;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use std::io::Cursor;
use std::path::Path;
use tokio::io::{AsyncRead, BufReader};

#[derive(Parser, Debug)]
#[command(name = "rwgt", version, about = "A streaming download and extraction tool")]
struct Args {
    /// Download URL
    url: String,

    /// Target output filename or path (used when downloading directly)
    #[arg(short = 'o')]
    output: Option<String>,

    /// Target directory for downloaded or extracted files
    #[arg(short = 'd', default_value = ".")]
    directory: String,

    /// Automatically extract compressed files
    #[arg(short = 'u')]
    unzip: bool,

    /// Set permission to 755 for downloaded or extracted files (Unix only)
    #[arg(short = 'x')]
    executable: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = Client::new();
    let response = client
        .get(&args.url)
        .send()
        .await
        .context("Failed to connect to target URL")?;

    if !response.status().is_success() {
        anyhow::bail!("Download failed, HTTP Status: {}", response.status());
    }

    let total_size = response.content_length();
    let mut download_stream = response.bytes_stream();

    let pb = if let Some(size) = total_size {
        ProgressBar::new(size)
    } else {
        ProgressBar::new_spinner()
    };
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}")?
            .progress_chars("#>-")
    );

    let mut buffer = Vec::new();
    while let Some(chunk) = download_stream.next().await {
        let chunk = chunk.context("Error while downloading chunk")?;
        buffer.extend_from_slice(&chunk);
        pb.inc(chunk.len() as u64);
    }
    pb.finish_with_message("Download completed");

    let cursor = Cursor::new(buffer);
    let buf_reader = BufReader::new(cursor);

    let url_filename = extract_filename_from_url(&args.url);
    let lower_name = url_filename.to_lowercase();

    if args.unzip {
        println!("Action: Streaming extraction");
        println!("Target Directory: {}", args.directory);

        if lower_name.ends_with(".tar.gz") || lower_name.ends_with(".tgz") {
            let decoder = GzipDecoder::new(buf_reader);
            unpack_tar(decoder, &args.directory, args.executable).await?;
        } else if lower_name.ends_with(".tar.xz") {
            let decoder = XzDecoder::new(buf_reader);
            unpack_tar(decoder, &args.directory, args.executable).await?;
        } else if lower_name.ends_with(".tar.bz2") {
            let decoder = BzDecoder::new(buf_reader);
            unpack_tar(decoder, &args.directory, args.executable).await?;
        } else if lower_name.ends_with(".tar.zst") {
            let decoder = ZstdDecoder::new(buf_reader);
            unpack_tar(decoder, &args.directory, args.executable).await?;
        } else if lower_name.ends_with(".zip") {
            unpack_zip(buf_reader, &args.directory, args.executable).await?;
        } else if lower_name.ends_with(".7z") {
            let inner_vec = buf_reader.into_inner().into_inner();
            unpack_7z(inner_vec, &args.directory, args.executable)?;
        } else if lower_name.ends_with(".gz") {
            let mut decoder = GzipDecoder::new(buf_reader);
            let out_name = if url_filename.ends_with(".gz") {
                &url_filename[..url_filename.len() - 3]
            } else {
                "extracted_file"
            };
            let final_path = Path::new(&args.directory).join(out_name);
            
            if let Some(parent) = final_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            
            let mut out_file = tokio::fs::File::create(&final_path).await?;
            tokio::io::copy(&mut decoder, &mut out_file).await?;
            println!("Extracted: {:?}", final_path);
            if args.executable {
                set_executable(&final_path)?;
            }
        } else {
            anyhow::bail!("Unsupported compression format for auto-extraction");
        }
    } else {
        let final_filename = args.output.unwrap_or(url_filename);
        let final_path = Path::new(&args.directory).join(final_filename);

        println!("Action: Direct Save");
        println!("Save Path: {:?}", final_path);

        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let inner_vec = buf_reader.into_inner().into_inner();
        tokio::fs::write(&final_path, inner_vec).await?;

        if args.executable {
            set_executable(&final_path)?;
        }
    }

    println!("Success!");
    Ok(())
}

fn extract_filename_from_url(url: &str) -> String {
    if let Ok(parsed) = reqwest::Url::parse(url) {
        if let Some(segments) = parsed.path_segments() {
            if let Some(last) = segments.last() {
                if !last.is_empty() {
                    return last.to_string();
                }
            }
        }
    }
    "downloaded_file".to_string()
}

async fn unpack_tar<R: AsyncRead + Unpin>(reader: R, target_dir: &str, set_exec: bool) -> Result<()> {
    let mut archive = tokio_tar::Archive::new(reader);
    let mut entries = archive.entries()?;
    tokio::fs::create_dir_all(target_dir).await?;

    while let Some(entry) = entries.next().await {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();

        if path.to_str().map_or(false, |s| s.starts_with("..") || s.starts_with('/')) {
            continue;
        }

        entry.unpack_in(target_dir).await?;
        let final_path = Path::new(target_dir).join(path);
        println!("Extracted: {:?}", final_path);

        if set_exec {
            set_executable(&final_path)?;
        }
    }
    Ok(())
}

async fn unpack_zip<R: AsyncRead + Unpin>(reader: R, target_dir: &str, set_exec: bool) -> Result<()> {
    use tokio_util::compat::TokioAsyncReadCompatExt;
    let compat_reader = reader.compat();
    let mut zip_reader = async_zip::base::read::stream::ZipFileReader::new(compat_reader);
    tokio::fs::create_dir_all(target_dir).await?;

    loop {
        match zip_reader.next_with_entry().await? {
            None => break,
            Some(mut entry_reader) => {
                let filename_str = entry_reader.reader().entry().filename().as_str()?;
                let filename_owned = filename_str.to_string();
                let rel_path = Path::new(&filename_owned);

                if rel_path.to_str().map_or(false, |s| s.starts_with("..") || s.starts_with('/')) {
                    zip_reader = entry_reader.skip().await?;
                    continue;
                }

                let final_path = Path::new(target_dir).join(rel_path);
                let is_dir = entry_reader.reader().entry().dir()?;

                if is_dir {
                    tokio::fs::create_dir_all(&final_path).await?;
                    zip_reader = entry_reader.skip().await?;
                } else {
                    if let Some(parent) = final_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }

                    let file = std::fs::File::create(&final_path)?;
                    let mut futures_writer = futures::io::AllowStdIo::new(file);
                    let mut actual_reader = entry_reader.reader_mut();

                    futures::io::copy(&mut actual_reader, &mut futures_writer)
                        .await
                        .context("Failed to write entry inside ZIP")?;

                    println!("Extracted: {:?}", final_path);

                    if set_exec {
                        set_executable(&final_path)?;
                    }

                    zip_reader = entry_reader.done().await?;
                }
            }
        }
    }
    Ok(())
}

fn unpack_7z(buffer: Vec<u8>, target_dir: &str, set_exec: bool) -> Result<()> {
    let mut cursor = Cursor::new(buffer);
    let target_path = Path::new(target_dir);
    std::fs::create_dir_all(target_path)?;

    // Pass sevenz_rust::Password::empty() as the 3rd argument
    let mut archive = sevenz_rust::SevenZReader::new(
        &mut cursor, 
        get_sevenz_len_hint(target_dir), 
        sevenz_rust::Password::empty()
    ).map_err(|e| anyhow::anyhow!("Failed to open 7z archive: {:?}", e))?;

    archive.for_each_entries(|entry, reader| {
        let rel_path = Path::new(entry.name());
        if rel_path.to_str().map_or(false, |s| s.starts_with("..") || s.starts_with('/')) {
            return Ok(true);
        }

        let final_path = target_path.join(rel_path);

        if entry.is_directory() {
            std::fs::create_dir_all(&final_path)?;
        } else {
            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::File::create(&final_path)?;
            std::io::copy(reader, &mut file)?;
            println!("Extracted: {:?}", final_path);

            if set_exec {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&final_path, std::fs::Permissions::from_mode(0o755));
                }
            }
        }
        Ok(true)
    }).map_err(|e| anyhow::anyhow!("Failed to extract 7z entry: {:?}", e))?;

    Ok(())
}

fn get_sevenz_len_hint(_: &str) -> u64 {
    0
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .context("Failed to set executable permissions (755)")?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}
