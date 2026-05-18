use std::fs;
use std::path::PathBuf;
use std::process::Command;

use agentdp_core::Context;
use agentdp_core::platform::{self, PlatformPaths};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceImage {
    pub url: &'static str,
    pub cache_key: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub source: SourceImage,
    pub cache_dir: PathBuf,
    pub image_path: PathBuf,
    pub download_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    AlreadyPresent,
    Downloaded,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to create image cache directory {path}: {source}")]
    CreateCacheDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("base image downloader curl was not found on PATH")]
    MissingDownloader,
    #[error("failed to remove partial base image download {path}: {source}")]
    RemovePartial {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to run curl for {url}: {source}")]
    RunDownloader {
        url: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("curl failed for {url}: {stderr}")]
    DownloadFailed { url: &'static str, stderr: String },
    #[error("downloaded base image {path} is empty")]
    EmptyDownload { path: PathBuf },
    #[error("cached base image {path} is empty")]
    EmptyCachedImage { path: PathBuf },
    #[error("failed to read base image metadata {path}: {source}")]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to move downloaded base image from {source_path} to {destination_path}: {source}")]
    MoveDownload {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[must_use]
pub fn plan(paths: &PlatformPaths, source: SourceImage) -> Plan {
    let cache_dir = paths.cache.join("images");
    let image_path = cache_dir.join(source.cache_key);
    let download_path = cache_dir.join(format!("{}.part", source.cache_key));
    Plan {
        source,
        cache_dir,
        image_path,
        download_path,
    }
}

fn ensure_cache_directory(plan: &Plan) -> Result<(), Error> {
    fs::create_dir_all(&plan.cache_dir).map_err(|source| Error::CreateCacheDirectory {
        path: plan.cache_dir.clone(),
        source,
    })
}

/// Ensures the image described by `plan` exists in the local cache.
///
/// # Errors
///
/// Returns an error if the cache directory cannot be created, an existing cache
/// entry is invalid, the downloader is unavailable, or the download fails.
pub fn ensure_cached(context: &Context, plan: &Plan) -> Result<Status, Error> {
    ensure_cache_directory(plan)?;
    if plan.image_path.exists() {
        let metadata = fs::metadata(&plan.image_path).map_err(|source| Error::Metadata {
            path: plan.image_path.clone(),
            source,
        })?;
        if metadata.len() == 0 {
            return Err(Error::EmptyCachedImage {
                path: plan.image_path.clone(),
            });
        }
        context
            .logger()
            .verbose_with(|| format!("base image already cached at {}", plan.image_path.display()));
        return Ok(Status::AlreadyPresent);
    }

    if plan.download_path.exists() {
        fs::remove_file(&plan.download_path).map_err(|source| Error::RemovePartial {
            path: plan.download_path.clone(),
            source,
        })?;
    }

    let curl = platform::find_binary("curl").ok_or(Error::MissingDownloader)?;
    context.logger().info(format!(
        "downloading base image {} to {}",
        plan.source.url,
        plan.image_path.display()
    ));
    let output = Command::new(curl)
        .args(["--fail", "--location", "--show-error", "--silent", "--output"])
        .arg(&plan.download_path)
        .arg(plan.source.url)
        .output()
        .map_err(|source| Error::RunDownloader {
            url: plan.source.url,
            source,
        })?;
    if !output.status.success() {
        return Err(Error::DownloadFailed {
            url: plan.source.url,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    let metadata = fs::metadata(&plan.download_path).map_err(|source| Error::Metadata {
        path: plan.download_path.clone(),
        source,
    })?;
    if metadata.len() == 0 {
        return Err(Error::EmptyDownload {
            path: plan.download_path.clone(),
        });
    }

    fs::rename(&plan.download_path, &plan.image_path).map_err(|source| Error::MoveDownload {
        source_path: plan.download_path.clone(),
        destination_path: plan.image_path.clone(),
        source,
    })?;
    Ok(Status::Downloaded)
}
