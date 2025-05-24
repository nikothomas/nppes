/*!
 * Download functionality for NPPES data from the internet
 * 
 * This module provides functionality to download NPPES data files directly
 * from CMS and other sources, including automatic ZIP extraction.
 */

#[cfg(feature = "download")]
use std::path::{Path, PathBuf};
#[cfg(feature = "download")]
use std::io::Write;
#[cfg(feature = "download")]
use reqwest;
#[cfg(feature = "download")]
use tokio;
#[cfg(feature = "download")]
use tempfile::TempDir;

#[cfg(feature = "progress")]
use indicatif::{ProgressBar, ProgressStyle};

use crate::{Result, NppesError};

/// Download configuration
#[cfg(feature = "download")]
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// Timeout for HTTP requests in seconds
    pub timeout_seconds: u64,
    /// Maximum file size to download in bytes
    pub max_file_size: Option<u64>,
    /// Whether to verify SSL certificates
    pub verify_ssl: bool,
    /// Custom user agent string
    pub user_agent: Option<String>,
    /// Directory to store downloaded files (None for temp directory)
    pub download_dir: Option<PathBuf>,
    /// Whether to keep downloaded files after processing
    pub keep_files: bool,
}

#[cfg(feature = "download")]
impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 300, // 5 minutes
            max_file_size: Some(20 * 1024 * 1024 * 1024), // 20GB
            verify_ssl: true,
            user_agent: Some(format!("nppes-rust/{}", env!("CARGO_PKG_VERSION"))),
            download_dir: None,
            keep_files: false,
        }
    }
}

/// Download manager for NPPES data
#[cfg(feature = "download")]
pub struct NppesDownloader {
    config: DownloadConfig,
    client: Option<reqwest::Client>,
}

#[cfg(feature = "download")]
impl NppesDownloader {
    /// Create a new downloader with default configuration
    pub fn new() -> Self {
        Self {
            config: DownloadConfig::default(),
            client: None,
        }
    }
    
    /// Create a new downloader with custom configuration
    pub fn with_config(config: DownloadConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }
    
    /// Get or create HTTP client
    async fn get_client(&mut self) -> Result<&reqwest::Client> {
        if self.client.is_none() {
            let mut builder = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(self.config.timeout_seconds))
                .danger_accept_invalid_certs(!self.config.verify_ssl);
            
            if let Some(user_agent) = &self.config.user_agent {
                builder = builder.user_agent(user_agent.as_str());
            }
            
            self.client = Some(builder.build().map_err(|e| NppesError::Custom {
                message: format!("Failed to create HTTP client: {}", e),
                suggestion: Some("Check your network configuration".to_string()),
            })?);
        }
        
        Ok(self.client.as_ref().unwrap())
    }
    
    /// Download a file from a URL
    pub async fn download_file(&mut self, url: &str, filename: Option<&str>) -> Result<PathBuf> {
        println!("Downloading from: {}", url);
        // Move config fields out before borrowing self mutably
        let max_file_size = self.config.max_file_size;
        let download_dir_opt = self.config.download_dir.clone();
        let verify_ssl = self.config.verify_ssl;
        let timeout_seconds = self.config.timeout_seconds;
        let user_agent = self.config.user_agent.clone();

        let client = if self.client.is_none() {
            let mut builder = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_seconds))
                .danger_accept_invalid_certs(!verify_ssl);
            if let Some(user_agent) = &user_agent {
                builder = builder.user_agent(user_agent.as_str());
            }
            self.client = Some(builder.build().map_err(|e| NppesError::Custom {
                message: format!("Failed to create HTTP client: {}", e),
                suggestion: Some("Check your network configuration".to_string()),
            })?);
        };
        let client = self.get_client().await?;
        
        // Make initial request to get content length
        let response = client.head(url).send().await.map_err(|e| {
            NppesError::Custom {
                message: format!("Failed to connect to URL: {}", e),
                suggestion: Some("Check the URL and your internet connection".to_string()),
            }
        })?;
        
        if !response.status().is_success() {
            return Err(NppesError::Custom {
                message: format!("HTTP error {}: {}", response.status(), url),
                suggestion: Some("Check if the URL is correct and accessible".to_string()),
            });
        }
        
        let content_length = response.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|ct_len| ct_len.to_str().ok())
            .and_then(|ct_len| ct_len.parse().ok());
        
        // Check file size limit
        if let (Some(max_size), Some(size)) = (max_file_size, content_length) {
            if size > max_size {
                return Err(NppesError::Custom {
                    message: format!(
                        "File size {} exceeds maximum allowed size {}",
                        format_bytes(size as usize),
                        format_bytes(max_size as usize)
                    ),
                    suggestion: Some("Increase max_file_size in DownloadConfig or download manually".to_string()),
                });
            }
        }
        
        // Determine download directory
        let download_dir = if let Some(dir) = &download_dir_opt {
            std::fs::create_dir_all(dir)?;
            dir.clone()
        } else {
            std::env::temp_dir()
        };
        
        // Determine filename
        let file_name = filename.unwrap_or_else(|| {
            url.split('/').last().unwrap_or("nppes_download")
        });
        
        let file_path = download_dir.join(file_name);
        
        // Start actual download
        let response = client.get(url).send().await.map_err(|e| {
            NppesError::Custom {
                message: format!("Failed to download file: {}", e),
                suggestion: Some("Check your internet connection and try again".to_string()),
            }
        })?;
        
        if !response.status().is_success() {
            return Err(NppesError::Custom {
                message: format!("HTTP error {}: {}", response.status(), url),
                suggestion: Some("Check if the URL is correct and accessible".to_string()),
            });
        }
        
        let mut file = tokio::fs::File::create(&file_path).await?;
        
        #[cfg(feature = "progress")]
        let progress_bar = if let Some(total_size) = content_length {
            let pb = ProgressBar::new(total_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                    .unwrap()
                    .progress_chars("#>-")
            );
            Some(pb)
        } else {
            None
        };
        
        // Download with progress tracking
        let mut downloaded = 0u64;
        let mut stream = response.bytes_stream();
        
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| NppesError::Custom {
                message: format!("Error downloading chunk: {}", e),
                suggestion: Some("Try downloading again".to_string()),
            })?;
            
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
            downloaded += chunk.len() as u64;
            
            #[cfg(feature = "progress")]
            if let Some(ref pb) = progress_bar {
                pb.set_position(downloaded);
            }
        }
        
        #[cfg(feature = "progress")]
        if let Some(pb) = progress_bar {
            pb.finish_with_message("Download complete");
        }
        
        println!("Downloaded {} to {}", format_bytes(downloaded as usize), file_path.display());
        
        Ok(file_path)
    }
    
    /// Download and extract a ZIP file
    pub async fn download_and_extract_zip(&mut self, url: &str, extract_to: Option<&Path>) -> Result<ExtractedFiles> {
        // Download the ZIP file
        let zip_path = self.download_file(url, None).await?;
        
        // Extract the ZIP file
        let extracted = self.extract_zip(&zip_path, extract_to)?;
        
        // Clean up ZIP file if not keeping files
        if !self.config.keep_files {
            let _ = std::fs::remove_file(&zip_path);
        }
        
        Ok(extracted)
    }
    
    /// Extract a ZIP file
    pub fn extract_zip(&self, zip_path: &Path, extract_to: Option<&Path>) -> Result<ExtractedFiles> {
        use std::fs::File;
        use std::io::BufReader;
        use zip::ZipArchive;
        
        let file = File::open(zip_path)?;
        let reader = BufReader::new(file);
        let mut archive = ZipArchive::new(reader).map_err(|e| NppesError::Custom {
            message: format!("Failed to open ZIP file: {}", e),
            suggestion: Some("Check if the file is a valid ZIP archive".to_string()),
        })?;
        
        // Determine extraction directory
        let extract_dir = if let Some(dir) = extract_to {
            dir.to_path_buf()
        } else if let Some(dir) = &self.config.download_dir {
            dir.clone()
        } else {
            std::env::temp_dir()
        };
        
        std::fs::create_dir_all(&extract_dir)?;
        
        let mut extracted_files = ExtractedFiles {
            directory: extract_dir.clone(),
            files: Vec::new(),
            main_data_file: None,
            taxonomy_file: None,
            other_names_file: None,
            practice_locations_file: None,
            endpoints_file: None,
        };
        
        println!("Extracting ZIP file to: {}", extract_dir.display());
        
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).map_err(|e| NppesError::Custom {
                message: format!("Failed to read file from ZIP: {}", e),
                suggestion: None,
            })?;
            
            let file_path = extract_dir.join(file.name());
            
            // Create parent directories if needed
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            
            // Extract file
            let mut outfile = File::create(&file_path)?;
            std::io::copy(&mut file, &mut outfile)?;
            
            println!("Extracted: {}", file.name());
            
            // Categorize extracted files
            let filename = file.name().to_lowercase();
            if filename.contains("npidata_pfile") && filename.ends_with(".csv") {
                extracted_files.main_data_file = Some(file_path.clone());
            } else if filename.contains("nucc_taxonomy") && filename.ends_with(".csv") {
                extracted_files.taxonomy_file = Some(file_path.clone());
            } else if filename.contains("othername_pfile") && filename.ends_with(".csv") {
                extracted_files.other_names_file = Some(file_path.clone());
            } else if filename.contains("pl_pfile") && filename.ends_with(".csv") {
                extracted_files.practice_locations_file = Some(file_path.clone());
            } else if filename.contains("endpoint_pfile") && filename.ends_with(".csv") {
                extracted_files.endpoints_file = Some(file_path.clone());
            }
            
            extracted_files.files.push(file_path);
        }
        
        println!("Extracted {} files", extracted_files.files.len());
        
        Ok(extracted_files)
    }
    
    /// Download the latest NPPES data from CMS
    pub async fn download_latest_nppes(&mut self) -> Result<ExtractedFiles> {
        use chrono::Datelike;
        // Get current year and month
        let today = chrono::Utc::now();
        let year = today.year();
        let month = today.format("%B").to_string(); // Full month name, e.g., "May"
        // Construct the URL for the latest file (Version 2)
        let url = format!(
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_{}_{}_V2.zip",
            month, year
        );
        self.download_and_extract_zip(&url, None).await
    }
}

/// Information about extracted files
#[cfg(feature = "download")]
#[derive(Debug, Clone)]
pub struct ExtractedFiles {
    /// Directory where files were extracted
    pub directory: PathBuf,
    /// All extracted files
    pub files: Vec<PathBuf>,
    /// Main NPPES data file (if found)
    pub main_data_file: Option<PathBuf>,
    /// Taxonomy reference file (if found)
    pub taxonomy_file: Option<PathBuf>,
    /// Other names file (if found)
    pub other_names_file: Option<PathBuf>,
    /// Practice locations file (if found)
    pub practice_locations_file: Option<PathBuf>,
    /// Endpoints file (if found)
    pub endpoints_file: Option<PathBuf>,
}

#[cfg(feature = "download")]
impl ExtractedFiles {
    /// Check if main data file was found
    pub fn has_main_data(&self) -> bool {
        self.main_data_file.is_some()
    }
    
    /// Get a summary of found files
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        
        if self.main_data_file.is_some() {
            parts.push("Main Data");
        }
        if self.taxonomy_file.is_some() {
            parts.push("Taxonomy");
        }
        if self.other_names_file.is_some() {
            parts.push("Other Names");
        }
        if self.practice_locations_file.is_some() {
            parts.push("Practice Locations");
        }
        if self.endpoints_file.is_some() {
            parts.push("Endpoints");
        }
        
        if parts.is_empty() {
            "No recognized NPPES files found".to_string()
        } else {
            format!("Found: {}", parts.join(", "))
        }
    }
}

// Helper function to format bytes
fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit_index])
}

// Re-export types when feature is not enabled for better error messages
#[cfg(not(feature = "download"))]
pub struct DownloadConfig;

#[cfg(not(feature = "download"))]
pub struct NppesDownloader;

#[cfg(not(feature = "download"))]
pub struct ExtractedFiles;

#[cfg(not(feature = "download"))]
impl NppesDownloader {
    pub fn new() -> Self {
        Self
    }
    
    pub async fn download_latest_nppes(&mut self) -> Result<ExtractedFiles> {
        Err(NppesError::feature_required("download"))
    }
}

#[cfg(not(feature = "download"))]
impl Default for NppesDownloader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "download"))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{BufRead, BufReader};

    #[tokio::test]
    async fn test_download_and_read_nppes_file() {
        let mut downloader = NppesDownloader::new();
        let extracted = downloader.download_latest_nppes().await.expect("Download and extraction should succeed");
        assert!(extracted.has_main_data(), "Main NPPES data file should be found in the archive");
        let main_file = extracted.main_data_file.as_ref().expect("Main data file should exist");
        assert!(main_file.exists(), "Main data file should exist on disk");
        // Try to open and read the first line
        let file = fs::File::open(main_file).expect("Should be able to open main data file");
        let mut reader = BufReader::new(file);
        let mut first_line = String::new();
        let bytes_read = reader.read_line(&mut first_line).expect("Should be able to read from main data file");
        assert!(bytes_read > 0, "Main data file should not be empty");
        // Clean up: remove all extracted files
        for path in &extracted.files {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir_all(&extracted.directory);
    }
} 