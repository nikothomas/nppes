/*!
 * Configuration support for NPPES library
 * 
 * Provides runtime configuration options for customizing library behavior.
 */

use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

/// Global configuration for NPPES library
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NppesConfig {
    /// Whether to show progress bars during long operations
    #[serde(default = "default_enable_progress_bar")]
    pub enable_progress_bar: bool,
    
    /// Number of threads for parallel operations (None = use all available)
    #[serde(default)]
    pub parallel_threads: Option<usize>,
    
    /// Validation level for data parsing
    #[serde(default)]
    pub validation_level: ValidationLevel,
    
    /// Whether to build indexes automatically when loading data
    #[serde(default = "default_index_on_load")]
    pub index_on_load: bool,
    
    /// Default export format
    #[serde(default)]
    pub default_export_format: crate::ExportFormat,
    
    /// Whether to skip invalid records during parsing
    #[serde(default)]
    pub skip_invalid_records: bool,
    
    /// Memory limit for loading data (in bytes, None = no limit)
    #[serde(default)]
    pub memory_limit: Option<usize>,
    
    /// Default batch size for bulk operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    /// Temporary directory for intermediate files
    #[serde(default)]
    pub temp_dir: Option<PathBuf>,
}

/// Validation level for data parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationLevel {
    /// No validation, fastest parsing
    None,
    /// Basic validation (NPI format, required fields)
    Basic,
    /// Standard validation (recommended)
    Standard,
    /// Strict validation (all fields, may reject valid data)
    Strict,
}

impl Default for ValidationLevel {
    fn default() -> Self {
        ValidationLevel::Standard
    }
}

impl Default for NppesConfig {
    fn default() -> Self {
        Self {
            enable_progress_bar: default_enable_progress_bar(),
            parallel_threads: None,
            validation_level: ValidationLevel::Standard,
            index_on_load: default_index_on_load(),
            default_export_format: crate::ExportFormat::Json,
            skip_invalid_records: false,
            memory_limit: None,
            batch_size: default_batch_size(),
            temp_dir: None,
        }
    }
}

// Default value functions for serde
fn default_enable_progress_bar() -> bool {
    true
}

fn default_index_on_load() -> bool {
    true
}

fn default_batch_size() -> usize {
    10_000
}

impl NppesConfig {
    /// Create a new configuration with default settings
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Load configuration from environment variables
    /// 
    /// Supported environment variables:
    /// - `NPPES_PROGRESS_BAR`: "true" or "false"
    /// - `NPPES_PARALLEL_THREADS`: number or "auto"
    /// - `NPPES_VALIDATION_LEVEL`: "none", "basic", "standard", or "strict"
    /// - `NPPES_INDEX_ON_LOAD`: "true" or "false"
    /// - `NPPES_SKIP_INVALID`: "true" or "false"
    /// - `NPPES_MEMORY_LIMIT`: number in bytes
    /// - `NPPES_BATCH_SIZE`: number
    /// - `NPPES_TEMP_DIR`: directory path
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("NPPES_PROGRESS_BAR") {
            config.enable_progress_bar = val.to_lowercase() == "true";
        }
        
        if let Ok(val) = std::env::var("NPPES_PARALLEL_THREADS") {
            config.parallel_threads = match val.to_lowercase().as_str() {
                "auto" | "0" => None,
                num => num.parse().ok(),
            };
        }
        
        if let Ok(val) = std::env::var("NPPES_VALIDATION_LEVEL") {
            config.validation_level = match val.to_lowercase().as_str() {
                "none" => ValidationLevel::None,
                "basic" => ValidationLevel::Basic,
                "standard" => ValidationLevel::Standard,
                "strict" => ValidationLevel::Strict,
                _ => ValidationLevel::Standard,
            };
        }
        
        if let Ok(val) = std::env::var("NPPES_INDEX_ON_LOAD") {
            config.index_on_load = val.to_lowercase() == "true";
        }
        
        if let Ok(val) = std::env::var("NPPES_SKIP_INVALID") {
            config.skip_invalid_records = val.to_lowercase() == "true";
        }
        
        if let Ok(val) = std::env::var("NPPES_MEMORY_LIMIT") {
            config.memory_limit = val.parse().ok();
        }
        
        if let Ok(val) = std::env::var("NPPES_BATCH_SIZE") {
            if let Ok(size) = val.parse() {
                config.batch_size = size;
            }
        }
        
        if let Ok(val) = std::env::var("NPPES_TEMP_DIR") {
            config.temp_dir = Some(PathBuf::from(val));
        }
        
        config
    }
    
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        let config: Self = toml::from_str(&contents)
            .map_err(|e| crate::NppesError::Configuration {
                message: format!("Failed to parse config file: {}", e),
                suggestion: Some("Check that the file is valid TOML format".to_string()),
            })?;
        Ok(config)
    }
    
    /// Save configuration to a TOML file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> crate::Result<()> {
        let contents = toml::to_string_pretty(self)
            .map_err(|e| crate::NppesError::Configuration {
                message: format!("Failed to serialize config: {}", e),
                suggestion: None,
            })?;
        std::fs::write(path, contents)?;
        Ok(())
    }
    
    /// Get the default configuration file path
    /// 
    /// Returns `~/.config/nppes/config.toml` on Unix-like systems
    /// or `%APPDATA%\nppes\config.toml` on Windows
    pub fn default_config_path() -> Option<PathBuf> {
        directories::ProjectDirs::from("", "", "nppes")
            .map(|dirs| dirs.config_dir().join("config.toml"))
    }
    
    /// Load configuration from the default location, environment, or defaults
    /// 
    /// Priority order:
    /// 1. Default config file (if exists)
    /// 2. Environment variables
    /// 3. Built-in defaults
    pub fn load() -> Self {
        // Try loading from default config file first
        if let Some(config_path) = Self::default_config_path() {
            if config_path.exists() {
                if let Ok(config) = Self::from_file(&config_path) {
                    return config;
                }
            }
        }
        
        // Fall back to environment variables
        Self::from_env()
    }
    
    /// Create a configuration optimized for performance
    pub fn performance() -> Self {
        Self {
            enable_progress_bar: false,
            parallel_threads: None, // Use all available
            validation_level: ValidationLevel::Basic,
            index_on_load: true,
            default_export_format: crate::ExportFormat::Json,
            skip_invalid_records: true,
            memory_limit: None,
            batch_size: 50_000,
            temp_dir: None,
        }
    }
    
    /// Create a configuration optimized for safety and validation
    pub fn safe() -> Self {
        Self {
            enable_progress_bar: true,
            parallel_threads: Some(1), // Single-threaded for predictability
            validation_level: ValidationLevel::Strict,
            index_on_load: true,
            default_export_format: crate::ExportFormat::Json,
            skip_invalid_records: false,
            memory_limit: Some(8 * 1024 * 1024 * 1024), // 8GB limit
            batch_size: 1_000,
            temp_dir: None,
        }
    }
}

// Global configuration support
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref GLOBAL_CONFIG: RwLock<Option<NppesConfig>> = RwLock::new(None);
}

/// Set the global configuration
pub fn set_global_config(config: NppesConfig) {
    *GLOBAL_CONFIG.write().unwrap() = Some(config);
}

/// Get the global configuration (or default if not set)
pub fn global_config() -> NppesConfig {
    GLOBAL_CONFIG.read().unwrap()
        .as_ref()
        .cloned()
        .unwrap_or_else(NppesConfig::load)
}

/// Clear the global configuration
pub fn clear_global_config() {
    *GLOBAL_CONFIG.write().unwrap() = None;
}

/// Builder for customizing configuration
pub struct ConfigBuilder {
    config: NppesConfig,
}

impl ConfigBuilder {
    /// Start building a new configuration
    pub fn new() -> Self {
        Self {
            config: NppesConfig::default(),
        }
    }
    
    /// Set progress bar enabled
    pub fn progress_bar(mut self, enabled: bool) -> Self {
        self.config.enable_progress_bar = enabled;
        self
    }
    
    /// Set number of parallel threads
    pub fn parallel_threads(mut self, threads: Option<usize>) -> Self {
        self.config.parallel_threads = threads;
        self
    }
    
    /// Set validation level
    pub fn validation_level(mut self, level: ValidationLevel) -> Self {
        self.config.validation_level = level;
        self
    }
    
    /// Set index on load
    pub fn index_on_load(mut self, enabled: bool) -> Self {
        self.config.index_on_load = enabled;
        self
    }
    
    /// Set skip invalid records
    pub fn skip_invalid_records(mut self, skip: bool) -> Self {
        self.config.skip_invalid_records = skip;
        self
    }
    
    /// Set memory limit
    pub fn memory_limit(mut self, limit: Option<usize>) -> Self {
        self.config.memory_limit = limit;
        self
    }
    
    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }
    
    /// Set temporary directory
    pub fn temp_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.config.temp_dir = Some(dir.as_ref().to_path_buf());
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> NppesConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_config_defaults() {
        let config = NppesConfig::default();
        assert!(config.enable_progress_bar);
        assert!(config.index_on_load);
        assert_eq!(config.validation_level, ValidationLevel::Standard);
    }
    
    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .progress_bar(false)
            .parallel_threads(Some(4))
            .validation_level(ValidationLevel::Strict)
            .skip_invalid_records(true)
            .batch_size(20_000)
            .build();
        
        assert!(!config.enable_progress_bar);
        assert_eq!(config.parallel_threads, Some(4));
        assert_eq!(config.validation_level, ValidationLevel::Strict);
        assert!(config.skip_invalid_records);
        assert_eq!(config.batch_size, 20_000);
    }
} 