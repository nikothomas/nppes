/*!
 * Enhanced error handling for NPPES data library operations
 * 
 * Provides detailed error types with context, suggestions, and recovery guidance.
 */

use std::fmt;
use std::path::PathBuf;
use thiserror::Error;
use serde::{Serialize, Deserialize};

/// NPPES library result type
pub type Result<T> = std::result::Result<T, NppesError>;

/// Enhanced error types with context and suggestions
#[derive(Error, Debug)]
pub enum NppesError {
    /// File I/O errors with context
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
        context: ErrorContext,
    },
    
    /// CSV parsing errors with location information
    #[error("CSV parsing error at line {line:?}: {message}")]
    CsvParse {
        message: String,
        line: Option<usize>,
        column: Option<String>,
        context: ErrorContext,
    },
    
    /// Data validation errors with detailed information
    #[error("Data validation error: {message}")]
    DataValidation {
        message: String,
        field: Option<String>,
        value: Option<String>,
        context: ErrorContext,
    },
    
    /// File not found with suggestions
    #[error("File not found: {path}")]
    FileNotFound {
        path: PathBuf,
        suggestion: String,
    },
    
    /// Invalid NPI with format guidance
    #[error("Invalid NPI '{npi}': {reason}")]
    InvalidNpi {
        npi: String,
        reason: String,
        suggestion: String,
    },
    
    /// Invalid entity type with valid options
    #[error("Invalid entity type code '{code}'")]
    InvalidEntityType {
        code: String,
        valid_options: Vec<String>,
    },
    
    /// Schema mismatch with details
    #[error("Schema mismatch: {message}")]
    SchemaMismatch {
        message: String,
        expected_columns: Option<usize>,
        found_columns: Option<usize>,
        mismatched_column: Option<(usize, String, String)>,
    },
    
    /// Date parsing errors with format hints
    #[error("Date parsing error: {message}")]
    DateParse {
        message: String,
        value: String,
        expected_format: String,
    },
    
    /// Configuration errors
    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        suggestion: Option<String>,
    },
    
    /// Export errors
    #[error("Export error: {message}")]
    Export {
        message: String,
        format: ExportFormat,
        suggestion: Option<String>,
    },
    
    /// Memory estimation errors
    #[error("Memory error: {message}")]
    Memory {
        message: String,
        required_bytes: Option<usize>,
        available_bytes: Option<usize>,
    },
    
    /// Feature not enabled error
    #[error("Feature '{feature}' is not enabled")]
    FeatureNotEnabled {
        feature: String,
        enable_instruction: String,
    },
    
    /// Generic errors with custom message
    #[error("{message}")]
    Custom {
        message: String,
        suggestion: Option<String>,
    },
}

/// Error context providing additional information
#[derive(Debug, Default, Clone)]
pub struct ErrorContext {
    pub file_path: Option<PathBuf>,
    pub line_number: Option<usize>,
    pub column_name: Option<String>,
    pub record_npi: Option<String>,
}

/// Export format for error context
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum ExportFormat {
    #[default]
    Json,
    Csv,
    Parquet,
    Arrow,
    Sql,
}

impl fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportFormat::Json => write!(f, "JSON"),
            ExportFormat::Csv => write!(f, "CSV"),
            ExportFormat::Parquet => write!(f, "Parquet"),
            ExportFormat::Arrow => write!(f, "Arrow"),
            ExportFormat::Sql => write!(f, "SQL"),
        }
    }
}

impl NppesError {
    /// Create a file not found error with helpful suggestion
    pub fn file_not_found_with_suggestion(path: PathBuf) -> Self {
        let suggestion = if path.to_string_lossy().contains("npidata") {
            format!(
                "Check if the file exists at '{}'. Common NPPES files follow the pattern 'npidata_pfile_YYYYMMDD-YYYYMMDD.csv'. \
                You can download the latest file from https://download.cms.gov/nppes/NPI_Files.html",
                path.display()
            )
        } else if path.to_string_lossy().contains("taxonomy") {
            format!(
                "Check if the taxonomy file exists at '{}'. The NUCC taxonomy file can be downloaded from \
                https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40",
                path.display()
            )
        } else {
            format!(
                "Check if the file exists at '{}'. Make sure the path is correct and you have read permissions.",
                path.display()
            )
        };
        
        Self::FileNotFound { path, suggestion }
    }
    
    /// Create an invalid NPI error with validation details
    pub fn invalid_npi(npi: &str) -> Self {
        let (reason, suggestion) = if npi.is_empty() {
            ("NPI cannot be empty".to_string(), 
             "Provide a valid 10-digit NPI number".to_string())
        } else if npi.len() != 10 {
            (format!("NPI must be exactly 10 digits, found {}", npi.len()),
             "Ensure the NPI is exactly 10 digits without spaces or special characters".to_string())
        } else if !npi.chars().all(|c| c.is_ascii_digit()) {
            ("NPI must contain only digits".to_string(),
             "Remove any non-numeric characters from the NPI".to_string())
        } else {
            ("Invalid NPI format".to_string(),
             "Verify the NPI number is correct".to_string())
        };
        
        Self::InvalidNpi {
            npi: npi.to_string(),
            reason,
            suggestion,
        }
    }
    
    /// Create an invalid entity type error with valid options
    pub fn invalid_entity_type(code: &str) -> Self {
        Self::InvalidEntityType {
            code: code.to_string(),
            valid_options: vec![
                "1 (Individual)".to_string(),
                "2 (Organization)".to_string(),
            ],
        }
    }
    
    /// Create a schema mismatch error with detailed information
    pub fn schema_mismatch_detailed(
        expected_columns: usize,
        found_columns: usize,
        mismatched_column: Option<(usize, String, String)>,
    ) -> Self {
        let message = if let Some((index, expected, found)) = &mismatched_column {
            format!(
                "Column {} mismatch: expected '{}', found '{}'",
                index, expected, found
            )
        } else {
            format!(
                "Expected {} columns, found {}",
                expected_columns, found_columns
            )
        };
        
        Self::SchemaMismatch {
            message,
            expected_columns: Some(expected_columns),
            found_columns: Some(found_columns),
            mismatched_column,
        }
    }
    
    /// Create a date parsing error with format information
    pub fn date_parse_with_format(value: &str, expected_format: &str) -> Self {
        Self::DateParse {
            message: format!("Cannot parse '{}' as date", value),
            value: value.to_string(),
            expected_format: expected_format.to_string(),
        }
    }
    
    /// Create a memory error with size information
    pub fn insufficient_memory(required: usize, available: Option<usize>) -> Self {
        let message = if let Some(avail) = available {
            format!(
                "Insufficient memory: need {} bytes but only {} bytes available",
                format_bytes(required),
                format_bytes(avail)
            )
        } else {
            format!("Insufficient memory: need {} bytes", format_bytes(required))
        };
        
        Self::Memory {
            message,
            required_bytes: Some(required),
            available_bytes: available,
        }
    }
    
    /// Create a feature not enabled error
    pub fn feature_required(feature: &str) -> Self {
        let enable_instruction = match feature {
            "dataframe" => "Add 'nppes = { version = \"0.2\", features = [\"dataframe\"] }' to your Cargo.toml",
            "arrow-export" => "Add 'nppes = { version = \"0.2\", features = [\"arrow-export\"] }' to your Cargo.toml",
            "full-text-search" => "Add 'nppes = { version = \"0.2\", features = [\"full-text-search\"] }' to your Cargo.toml",
            _ => "Enable the required feature in your Cargo.toml",
        };
        
        Self::FeatureNotEnabled {
            feature: feature.to_string(),
            enable_instruction: enable_instruction.to_string(),
        }
    }
    
    /// Get a user-friendly error message with suggestions
    pub fn user_message(&self) -> String {
        match self {
            Self::FileNotFound { suggestion, .. } => {
                format!("{}\n\nSuggestion: {}", self, suggestion)
            }
            Self::InvalidNpi { suggestion, .. } => {
                format!("{}\n\nSuggestion: {}", self, suggestion)
            }
            Self::InvalidEntityType { valid_options, .. } => {
                format!("{}\n\nValid options: {}", self, valid_options.join(", "))
            }
            Self::DateParse { expected_format, .. } => {
                format!("{}\n\nExpected format: {}", self, expected_format)
            }
            Self::FeatureNotEnabled { enable_instruction, .. } => {
                format!("{}\n\nTo enable: {}", self, enable_instruction)
            }
            Self::Custom { suggestion: Some(sug), .. } => {
                format!("{}\n\nSuggestion: {}", self, sug)
            }
            _ => self.to_string(),
        }
    }
}

/// Format bytes into human-readable string
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

// Convenience conversions
impl From<std::io::Error> for NppesError {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            message: err.to_string(),
            source: err,
            context: ErrorContext::default(),
        }
    }
}

impl From<csv::Error> for NppesError {
    fn from(err: csv::Error) -> Self {
        let (line, message) = match err.position() {
            Some(pos) => (Some(pos.line() as usize), err.to_string()),
            None => (None, err.to_string()),
        };
        
        Self::CsvParse {
            message,
            line,
            column: None,
            context: ErrorContext::default(),
        }
    }
}

impl From<serde_json::Error> for NppesError {
    fn from(err: serde_json::Error) -> Self {
        NppesError::Export {
            message: err.to_string(),
            format: ExportFormat::Json,
            suggestion: Some("Check if the data is serializable to JSON.".to_string()),
        }
    }
}