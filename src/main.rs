use anyhow::{Context, Result};
use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder};
use clap::Parser;
// 核心修复 1: 必须引入 StreamExt，否则 tar 的 entries.next() 无法使用
use futures::stream::{StreamExt, TryStreamExt};
use reqwest::Client;
use std::path::Path;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_tar::Archive;
// 核心修复 2: 使用 tokio_util 实现从 futures Stream 到 tokio AsyncRead 的完美桥接
use tokio_util::io::StreamReader;

/// rwgt - 一个类似 wget 的流式下载解压工具
#[derive(Parser, Debug)]
#[command(name = "rwgt", version, about)]
struct Args {
    /// 下载链接
    url: String,

    /// 指定输出文件名 (如果不指定则从URL提取)
    #[arg(short = 'o')]
    output: Option<String>,

    /// 智能识别格式并流式解压下载的压缩包
    #[arg(short = 'u')]
    unzip: bool,

    /// 给下载或解压出的文件赋予 755 权限
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
        .context("无法连接到目标URL")?;

    if !response.status().is_success() {
        anyhow::bail!("下载失败，HTTP 状态码: {}", response.status());
    }

    // 获取文件名
    let filename = args.output.clone().unwrap_or_else(|| {
        extract_filename_from_url(&args.url)
    });

    println!("🚀 开始下载: {}", args.url);

    // 将 HTTP 流转换为 Tokio 兼容的 AsyncRead
    let stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    
    // 使用 StreamReader 直接构建满足 tokio::io::AsyncRead 的流
    let reader = StreamReader::new(stream);
    // 用 BufReader 包装，使其完美满足 tokio::io::AsyncBufRead (解压器所需要的特性)
    let buf_reader = BufReader::new(reader);

    if args.unzip {
        println!("📦 检测到 -u 参数，启动流式解压...");
        let lower_name = filename.to_lowercase();
        
        // 根据后缀智能选择解压流
        if lower_name.ends_with(".tar.gz") || lower_name.ends_with(".tgz") {
            let decoder = GzipDecoder::new(buf_reader);
            unpack_tar(decoder, args.executable).await?;
        } else if lower_name.ends_with(".tar.xz") {
            let decoder = XzDecoder::new(buf_reader);
            unpack_tar(decoder, args.executable).await?;
        } else if lower_name.ends_with(".tar.bz2") {
            let decoder = BzDecoder::new(buf_reader);
            unpack_tar(decoder, args.executable).await?;
        } else if lower_name.ends_with(".tar.zst") {
            let decoder = ZstdDecoder::new(buf_reader);
            unpack_tar(decoder, args.executable).await?;
        } else if lower_name.ends_with(".zip") {
            // ZIP 格式的流式解压
            unpack_zip(buf_reader, args.executable).await?;
        } else {
            println!("⚠️ 无法识别的压缩格式，按原始文件保存为: {}", filename);
            save_file(buf_reader, &filename, args.executable).await?;
        }
    } else {
        save_file(buf_reader, &filename, args.executable).await?;
    }

    println!("✅ 完成!");
    Ok(())
}

fn extract_filename_from_url(url: &str) -> String {
    url.split('/')
        .last()
        .unwrap_or("downloaded_file")
        .split('?')
        .next()
        .unwrap_or("downloaded_file")
        .to_string()
}

/// 流式保存单文件
async fn save_file<R: tokio::io::AsyncRead + Unpin>(mut reader: R, filename: &str, set_exec: bool) -> Result<()> {
    let path = Path::new(filename);
    let mut file = tokio::fs::File::create(path).await.context("创建文件失败")?;
    
    // 流式拷贝，不占用大内存
    tokio::io::copy(&mut reader, &mut file).await.context("写入文件失败")?;
    println!("💾 文件已保存至: {}", filename);

    if set_exec {
        set_executable(path)?;
    }
    Ok(())
}

/// 流式解包 tar 系列格式
async fn unpack_tar<R: tokio::io::AsyncRead + Unpin>(reader: R, set_exec: bool) -> Result<()> {
    let mut archive = Archive::new(reader);
    let mut entries = archive.entries()?;
    
    while let Some(entry) = entries.next().await {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        
        // 确保解压路径安全，防止路径遍历攻击
        if path.to_str().map_or(false, |s| s.starts_with("..") || s.starts_with('/')) {
            continue;
        }

        entry.unpack_in(".").await?;
        println!("📄 解压出: {:?}", path);

        if set_exec {
            set_executable(&path)?;
        }
    }
    Ok(())
}

/// 流式解包 zip 格式
async fn unpack_zip<R: tokio::io::AsyncRead + Unpin>(reader: R, set_exec: bool) -> Result<()> {
    use tokio_util::compat::TokioAsyncReadCompatExt;
    
    let compat_reader = reader.compat();
    // 1. 这里不能用 while let，必须用 loop，因为我们要手动控制这个不可变状态机的生命周期接力
    let mut zip_reader = async_zip::base::read::stream::ZipFileReader::new(compat_reader);

    loop {
        // 消费旧的 zip_reader，进化为包含 Reading 状态的新 zip_reader
        match zip_reader.next_with_entry().await? {
            None => break, // 压缩包读完了，优雅退出
            Some(mut entry_reader) => {
                
                // 核心修复 1：拿到 &str 后立刻 to_string() 切断引用链，释放 entry_reader 的不可变借用
                let filename_str = entry_reader.reader().entry().filename().as_str()?;
                let filename_owned = filename_str.to_string();
                let path = Path::new(&filename_owned);
                
                // 核心修复 2：提前把我们需要判断的 bool 值存下来
                let is_dir = entry_reader.reader().entry().dir()?;

                if path.to_str().map_or(false, |s| s.starts_with("..") || s.starts_with('/')) {
                    // 如果跳过，也必须把状态机转回 Ready 状态接力给下一次循环，否则就漏掉了
                    zip_reader = entry_reader.skip().await?;
                    continue;
                }

                if is_dir {
                    tokio::fs::create_dir_all(path).await?;
                    // 目录处理完，将状态机恢复为 Ready 传给下一轮
                    zip_reader = entry_reader.skip().await?;
                } else {
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }

                    let file = std::fs::File::create(path)?;
                    let mut futures_writer = futures::io::AllowStdIo::new(file);

                    // 此时 entry_reader 身上已经没有任何不可变借用了，可以安全地调用 reader_mut()
                    let mut actual_reader = entry_reader.reader_mut();

                    futures::io::copy(&mut actual_reader, &mut futures_writer)
                        .await
                        .context("解压并写入 ZIP 内部文件失败")?;
                    
                    println!("📄 解压出: {:?}", path);

                    if set_exec {
                        set_executable(path)?;
                    }

                    // 核心修复 3：数据读完后，调用 done() 方法关闭当前文件的读取锁
                    // 它会吞掉 entry_reader 并安全地返回一个恢复到 Ready 状态的最初的 ZipFileReader
                    zip_reader = entry_reader.done().await?;
                }
            }
        }
    }
    Ok(())
}

/// 给文件赋予 755 权限
#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .context(format!("设置权限失败: {:?}", path))?;
    println!("🔓 已赋予 755 权限: {:?}", path);
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    println!("⚠️ 当前系统不支持 Unix 权限设置，已忽略 -x 参数");
    Ok(())
}
