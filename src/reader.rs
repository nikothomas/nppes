/*!
 * Enhanced CSV reader for NPPES data files
 * 
 * This module provides functionality to read and parse NPPES CSV files
 * into structured data types with proper validation, progress reporting,
 * and memory usage estimation.
 */

use std::path::Path;
use std::fs::File;
use std::time::{Duration, Instant};
use csv::ReaderBuilder;
use chrono::NaiveDate;

#[cfg(feature = "progress")]
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    Result, NppesError, ErrorContext,
    data_types::*,
    schema::*,
    constants::*,
};

/// Progress information for long-running operations
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    /// Number of records processed so far
    pub current_records: usize,
    /// Estimated total number of records (if known)
    pub estimated_total: Option<usize>,
    /// Number of bytes processed
    pub bytes_processed: usize,
    /// Time elapsed since operation started
    pub elapsed_time: Duration,
    /// Estimated time remaining (if calculable)
    pub estimated_remaining: Option<Duration>,
    /// Current processing rate (records per second)
    pub records_per_second: f64,
}

/// Memory usage estimation
#[derive(Debug, Clone)]
pub struct MemoryEstimate {
    /// Size of the source file in bytes
    pub file_size: u64,
    /// Estimated number of records
    pub estimated_records: u64,
    /// Estimated memory needed in bytes
    pub estimated_memory_bytes: usize,
    /// Human-readable memory estimate
    pub estimated_memory_human: String,
}

/// Enhanced NPPES data reader with CSV parsing capabilities
pub struct NppesReader {
    /// Whether to validate CSV headers against expected schema
    validate_headers: bool,
    /// Whether to skip invalid records (true) or fail on first error (false)
    skip_invalid_records: bool,
    /// Progress callback function
    #[cfg(feature = "progress")]
    progress_callback: Option<Box<dyn Fn(ProgressInfo) + Send + Sync>>,
    /// Whether to show progress bar
    #[cfg(feature = "progress")]
    show_progress_bar: bool,
}

impl Default for NppesReader {
    fn default() -> Self {
        Self::new()
    }
}

impl NppesReader {
    /// Create a new NPPES reader with default settings
    pub fn new() -> Self {
        Self {
            validate_headers: true,
            skip_invalid_records: false,
            #[cfg(feature = "progress")]
            progress_callback: None,
            #[cfg(feature = "progress")]
            show_progress_bar: true,
        }
    }
    
    /// Enable or disable header validation
    pub fn with_header_validation(mut self, validate: bool) -> Self {
        self.validate_headers = validate;
        self
    }
    
    /// Enable or disable skipping invalid records
    pub fn with_skip_invalid_records(mut self, skip: bool) -> Self {
        self.skip_invalid_records = skip;
        self
    }
    
    #[cfg(feature = "progress")]
    /// Set a progress callback function
    pub fn with_progress<F>(mut self, callback: F) -> Self 
    where 
        F: Fn(ProgressInfo) + Send + Sync + 'static
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }
    
    #[cfg(feature = "progress")]
    /// Enable or disable the progress bar
    pub fn with_progress_bar(mut self, show: bool) -> Self {
        self.show_progress_bar = show;
        self
    }
    
    /// Estimate memory usage for a file
    pub fn estimate_memory_usage<P: AsRef<Path>>(path: P) -> Result<MemoryEstimate> {
        let path = path.as_ref();
        let metadata = std::fs::metadata(path)?;
        let file_size = metadata.len();
        
        // Estimate based on typical compression ratio and record size
        // NPPES records average about 2KB in CSV, 500 bytes in memory
        let estimated_records = file_size / 2000;
        let estimated_memory_bytes = (estimated_records as usize) * 500;
        
        let estimated_memory_human = format_bytes(estimated_memory_bytes);
        
        Ok(MemoryEstimate {
            file_size,
            estimated_records,
            estimated_memory_bytes,
            estimated_memory_human,
        })
    }
    
    /// Check if there's enough memory to load a file
    pub fn check_memory_availability<P: AsRef<Path>>(path: P) -> Result<bool> {
        let estimate = Self::estimate_memory_usage(path)?;
        
        // Get available system memory (platform-specific)
        #[cfg(target_os = "windows")]
        let available_memory = get_available_memory_windows();
        
        #[cfg(not(target_os = "windows"))]
        let available_memory = get_available_memory_unix();
        
        if let Some(available) = available_memory {
            // Leave at least 1GB free
            let buffer = 1_073_741_824;
            if estimate.estimated_memory_bytes + buffer > available {
                return Err(NppesError::insufficient_memory(
                    estimate.estimated_memory_bytes,
                    Some(available)
                ));
            }
        }
        
        Ok(true)
    }
    
    /// Load the main NPPES provider data from CSV file
    pub fn load_main_data<P: AsRef<Path>>(&self, path: P) -> Result<Vec<NppesRecord>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NppesError::file_not_found_with_suggestion(path.to_path_buf()));
        }
        
        // Check memory availability
        let memory_estimate = Self::estimate_memory_usage(path)?;
        println!("Estimated memory usage: {}", memory_estimate.estimated_memory_human);
        
        Self::check_memory_availability(path)?;
        
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        // Validate headers if enabled
        if self.validate_headers {
            let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
            NppesMainSchema::validate_headers(&headers)?;
        }
        
        let mut records = Vec::with_capacity(memory_estimate.estimated_records as usize);
        let mut record_count = 0;
        let mut bytes_processed = 0;
        let mut invalid_count = 0;
        let start_time = Instant::now();
        
        #[cfg(feature = "progress")]
        let progress_bar = if self.show_progress_bar {
            let pb = ProgressBar::new(file_size);
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
        
        for result in reader.records() {
            record_count += 1;
            
            // Update progress
            let elapsed = start_time.elapsed();
            let records_per_second = if elapsed.as_secs() > 0 {
                record_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            
            // Estimate bytes processed (rough approximation)
            bytes_processed = (record_count * 2000).min(file_size as usize);
            
            #[cfg(feature = "progress")]
            {
                if let Some(ref pb) = progress_bar {
                    pb.set_position(bytes_processed as u64);
                }
                
                if let Some(ref callback) = self.progress_callback {
                    if record_count % 1000 == 0 {
                        let progress = ProgressInfo {
                            current_records: record_count,
                            estimated_total: Some(memory_estimate.estimated_records as usize),
                            bytes_processed,
                            elapsed_time: elapsed,
                            estimated_remaining: estimate_remaining_time(
                                record_count,
                                memory_estimate.estimated_records as usize,
                                elapsed
                            ),
                            records_per_second,
                        };
                        callback(progress);
                    }
                }
            }
            
            match result {
                Ok(csv_record) => {
                    match self.parse_main_record(&csv_record, record_count) {
                        Ok(record) => records.push(record),
                        Err(e) => {
                            invalid_count += 1;
                            if self.skip_invalid_records {
                                if invalid_count <= 10 {
                                    eprintln!("Warning: Skipping invalid record {}: {}", record_count, e);
                                }
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    let error = NppesError::CsvParse {
                        message: format!("CSV error: {}", e),
                        line: Some(record_count),
                        column: None,
                        context: ErrorContext {
                            file_path: Some(path.to_path_buf()),
                            line_number: Some(record_count),
                            ..Default::default()
                        },
                    };
                    
                    if self.skip_invalid_records {
                        invalid_count += 1;
                        if invalid_count <= 10 {
                            eprintln!("Warning: {}", error);
                        }
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        
        #[cfg(feature = "progress")]
        if let Some(pb) = progress_bar {
            pb.finish_with_message("Loading complete");
        }
        
        let elapsed = start_time.elapsed();
        
        #[cfg(feature = "progress")]
        if self.show_progress_bar {
            println!(
                "Successfully loaded {} NPPES provider records in {:.2}s ({:.0} records/sec)",
                records.len(),
                elapsed.as_secs_f64(),
                records.len() as f64 / elapsed.as_secs_f64()
            );
            
            if invalid_count > 0 {
                println!("Skipped {} invalid records", invalid_count);
            }
        }
        
        #[cfg(not(feature = "progress"))]
        {
            println!(
                "Successfully loaded {} NPPES provider records in {:.2}s ({:.0} records/sec)",
                records.len(),
                elapsed.as_secs_f64(),
                records.len() as f64 / elapsed.as_secs_f64()
            );
            
            if invalid_count > 0 {
                println!("Skipped {} invalid records", invalid_count);
            }
        }
        
        Ok(records)
    }
    
    /// Load taxonomy reference data from CSV file
    pub fn load_taxonomy_data<P: AsRef<Path>>(&self, path: P) -> Result<Vec<TaxonomyReference>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NppesError::file_not_found_with_suggestion(path.to_path_buf()));
        }
        
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        // Validate headers if enabled
        if self.validate_headers {
            let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
            TaxonomySchema::validate_headers(&headers)?;
        }
        
        let mut records = Vec::new();
        let start_time = Instant::now();
        
        for (idx, result) in reader.records().enumerate() {
            let csv_record = result.map_err(|e| NppesError::CsvParse {
                message: e.to_string(),
                line: Some(idx + 2), // +2 for header and 0-based index
                column: None,
                context: ErrorContext {
                    file_path: Some(path.to_path_buf()),
                    line_number: Some(idx + 2),
                    ..Default::default()
                },
            })?;
            
            let record = self.parse_taxonomy_record(&csv_record)?;
            records.push(record);
        }
        
        let elapsed = start_time.elapsed();
        
        #[cfg(feature = "progress")]
        if self.show_progress_bar {
            println!(
                "Successfully loaded {} taxonomy reference records in {:.2}s",
                records.len(),
                elapsed.as_secs_f64()
            );
        }
        
        #[cfg(not(feature = "progress"))]
        println!(
            "Successfully loaded {} taxonomy reference records in {:.2}s",
            records.len(),
            elapsed.as_secs_f64()
        );
        
        Ok(records)
    }
    
    /// Load other name reference data from CSV file
    pub fn load_other_name_data<P: AsRef<Path>>(&self, path: P) -> Result<Vec<OtherNameRecord>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NppesError::file_not_found_with_suggestion(path.to_path_buf()));
        }
        
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        if self.validate_headers {
            let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
            OtherNameSchema::validate_headers(&headers)?;
        }
        
        let mut records = Vec::new();
        let start_time = Instant::now();
        
        for (idx, result) in reader.records().enumerate() {
            let csv_record = result.map_err(|e| NppesError::CsvParse {
                message: e.to_string(),
                line: Some(idx + 2),
                column: None,
                context: ErrorContext {
                    file_path: Some(path.to_path_buf()),
                    line_number: Some(idx + 2),
                    ..Default::default()
                },
            })?;
            
            let record = self.parse_other_name_record(&csv_record)?;
            records.push(record);
        }
        
        let elapsed = start_time.elapsed();
        
        #[cfg(feature = "progress")]
        if self.show_progress_bar {
            println!(
                "Successfully loaded {} other name records in {:.2}s",
                records.len(),
                elapsed.as_secs_f64()
            );
        }
        
        #[cfg(not(feature = "progress"))]
        println!(
            "Successfully loaded {} other name records in {:.2}s",
            records.len(),
            elapsed.as_secs_f64()
        );
        
        Ok(records)
    }
    
    /// Load practice location reference data from CSV file
    pub fn load_practice_location_data<P: AsRef<Path>>(&self, path: P) -> Result<Vec<PracticeLocationRecord>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NppesError::file_not_found_with_suggestion(path.to_path_buf()));
        }
        
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        if self.validate_headers {
            let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
            PracticeLocationSchema::validate_headers(&headers)?;
        }
        
        let mut records = Vec::new();
        let start_time = Instant::now();
        
        for (idx, result) in reader.records().enumerate() {
            let csv_record = result.map_err(|e| NppesError::CsvParse {
                message: e.to_string(),
                line: Some(idx + 2),
                column: None,
                context: ErrorContext {
                    file_path: Some(path.to_path_buf()),
                    line_number: Some(idx + 2),
                    ..Default::default()
                },
            })?;
            
            let record = self.parse_practice_location_record(&csv_record)?;
            records.push(record);
        }
        
        let elapsed = start_time.elapsed();
        
        #[cfg(feature = "progress")]
        if self.show_progress_bar {
            println!(
                "Successfully loaded {} practice location records in {:.2}s",
                records.len(),
                elapsed.as_secs_f64()
            );
        }
        
        #[cfg(not(feature = "progress"))]
        println!(
            "Successfully loaded {} practice location records in {:.2}s",
            records.len(),
            elapsed.as_secs_f64()
        );
        
        Ok(records)
    }
    
    /// Load endpoint reference data from CSV file
    pub fn load_endpoint_data<P: AsRef<Path>>(&self, path: P) -> Result<Vec<EndpointRecord>> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(NppesError::file_not_found_with_suggestion(path.to_path_buf()));
        }
        
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        if self.validate_headers {
            let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
            EndpointSchema::validate_headers(&headers)?;
        }
        
        let mut records = Vec::new();
        let start_time = Instant::now();
        
        for (idx, result) in reader.records().enumerate() {
            let csv_record = result.map_err(|e| NppesError::CsvParse {
                message: e.to_string(),
                line: Some(idx + 2),
                column: None,
                context: ErrorContext {
                    file_path: Some(path.to_path_buf()),
                    line_number: Some(idx + 2),
                    ..Default::default()
                },
            })?;
            
            let record = self.parse_endpoint_record(&csv_record)?;
            records.push(record);
        }
        
        let elapsed = start_time.elapsed();
        
        #[cfg(feature = "progress")]
        if self.show_progress_bar {
            println!(
                "Successfully loaded {} endpoint records in {:.2}s",
                records.len(),
                elapsed.as_secs_f64()
            );
        }
        
        #[cfg(not(feature = "progress"))]
        println!(
            "Successfully loaded {} endpoint records in {:.2}s",
            records.len(),
            elapsed.as_secs_f64()
        );
        
        Ok(records)
    }
    
    /// Parse a main NPPES record from CSV row
    fn parse_main_record(&self, record: &csv::StringRecord, line_number: usize) -> Result<NppesRecord> {
        let get_field = |index: usize| -> Option<String> {
            record.get(index)
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
        };
        
        let get_required_field = |index: usize, field_name: &str| -> Result<String> {
            get_field(index).ok_or_else(|| {
                NppesError::DataValidation {
                    message: format!("Missing required field: {}", field_name),
                    field: Some(field_name.to_string()),
                    value: None,
                    context: ErrorContext {
                        line_number: Some(line_number),
                        ..Default::default()
                    },
                }
            })
        };
        
        // Core identifiers
        let npi_str = get_required_field(0, "NPI")?;
        let npi = Npi::new(npi_str.clone()).map_err(|_| NppesError::invalid_npi(&npi_str))?;
        
        let entity_type_str = get_field(1);
        let entity_type = match entity_type_str {
            Some(ref s) => EntityType::from_code(s).ok(),
            None => None,
        };
        
        let replacement_npi = get_field(2).map(|s| Npi::new(s)).transpose()
            .map_err(|e| e)?;
        let ein = get_field(3);
        
        // Provider names
        let provider_name = ProviderName {
            prefix: get_field(9).as_deref().and_then(NamePrefixCode::from_code),
            first: get_field(7),
            middle: get_field(8),
            last: get_field(6),
            suffix: get_field(10).as_deref().and_then(NameSuffixCode::from_code),
            credential: get_field(11),
        };
        
        let provider_other_name = ProviderName {
            prefix: get_field(17).as_deref().and_then(NamePrefixCode::from_code),
            first: get_field(15),
            middle: get_field(16),
            last: get_field(14),
            suffix: get_field(18).as_deref().and_then(NameSuffixCode::from_code),
            credential: get_field(19),
        };
        
        // Organization information
        let organization_name = OrganizationName {
            legal_business_name: get_field(4),
            other_name: get_field(12),
            other_name_type: get_field(13).as_deref().and_then(OtherProviderNameTypeCode::from_code),
        };
        
        // Addresses
        let mailing_address = Address {
            line_1: get_field(20),
            line_2: get_field(21),
            city: get_field(22),
            postal_code: get_field(24),
            telephone: get_field(26),
            fax: get_field(27),
            state: get_field(23).as_deref().and_then(StateCode::from_code),
            country: get_field(25).as_deref().map(CountryCode::from_code),
        };
        
        let practice_address = Address {
            line_1: get_field(28),
            line_2: get_field(29),
            city: get_field(30),
            postal_code: get_field(32),
            telephone: get_field(34),
            fax: get_field(35),
            state: get_field(31).as_deref().and_then(StateCode::from_code),
            country: get_field(33).as_deref().map(CountryCode::from_code),
        };
        
        // Dates
        let enumeration_date = get_field(36).map(|s| self.parse_date(&s)).transpose()?;
        let last_update_date = get_field(37).map(|s| self.parse_date(&s)).transpose()?;
        let deactivation_date = get_field(39).map(|s| self.parse_date(&s)).transpose()?;
        let reactivation_date = get_field(40).map(|s| self.parse_date(&s)).transpose()?;
        
        // Parse taxonomy codes (starting from column 47)
        let mut taxonomy_codes = Vec::new();
        for i in 0..MAX_TAXONOMY_CODES {
            let base_index = 47 + (i * 4);
            if let Some(code) = get_field(base_index) {
                let group_taxonomy_code = get_field(307 + i).as_deref().and_then(GroupTaxonomyCode::from_code);
                let primary_switch = get_field(base_index + 3).as_deref().and_then(PrimaryTaxonomySwitch::from_code);
                let taxonomy_code = TaxonomyCode {
                    code,
                    license_number: get_field(base_index + 1),
                    license_state: get_field(base_index + 2),
                    is_primary: get_field(base_index + 3)
                        .map(|s| s == "Y")
                        .unwrap_or(false),
                    taxonomy_group: get_field(307 + i),
                    group_taxonomy_code,
                    primary_switch,
                };
                taxonomy_codes.push(taxonomy_code);
            }
        }
        
        // Parse other identifiers (starting from column 107)
        let mut other_identifiers = Vec::new();
        for i in 0..MAX_OTHER_IDENTIFIERS {
            let base_index = 107 + (i * 4);
            if let Some(identifier) = get_field(base_index) {
                let state = get_field(base_index + 2).as_deref().and_then(StateCode::from_code);
                let issuer = get_field(base_index + 3).as_deref().and_then(OtherProviderIdentifierIssuerCode::from_code);
                let other_id = OtherIdentifier {
                    identifier,
                    type_code: get_field(base_index + 1),
                    issuer,
                    state,
                };
                other_identifiers.push(other_id);
            }
        }
        
        // Authorized official (for organizations)
        let authorized_official = if entity_type == Some(EntityType::Organization) {
            Some(AuthorizedOfficial {
                prefix: get_field(308).as_deref().and_then(NamePrefixCode::from_code),
                first_name: get_field(43),
                middle_name: get_field(44),
                last_name: get_field(42),
                suffix: get_field(309).as_deref().and_then(NameSuffixCode::from_code),
                credential: get_field(310),
                title: get_field(45),
                telephone: get_field(46),
            })
        } else {
            None
        };
        
        // Organization flags and parent info (near the end)
        let sole_proprietor = get_field(307).as_deref().and_then(SoleProprietorCode::from_code);
        let organization_subpart = get_field(308).as_deref().and_then(SubpartCode::from_code);
        let parent_organization_lbn = get_field(309);
        let parent_organization_tin = get_field(310);
        
        // Certification date (last column)
        let certification_date = get_field(329).map(|s| self.parse_date(&s)).transpose()?;
        
        // Deactivation reason and gender codes
        let deactivation_reason_code = get_field(38);
        let deactivation_reason = deactivation_reason_code.as_deref().and_then(DeactivationReasonCode::from_code);
        let provider_gender_code = get_field(41);
        let provider_gender = provider_gender_code.as_deref().and_then(SexCode::from_code);
        // Provider other name type code
        let provider_other_name_type_code = get_field(20);
        let provider_other_name_type = provider_other_name_type_code.as_deref().and_then(OtherProviderNameTypeCode::from_code);
        
        Ok(NppesRecord {
            npi,
            entity_type,
            replacement_npi,
            ein,
            provider_name,
            provider_other_name,
            provider_other_name_type,
            organization_name,
            mailing_address,
            practice_address,
            enumeration_date,
            last_update_date,
            deactivation_date,
            reactivation_date,
            certification_date,
            deactivation_reason,
            provider_gender,
            authorized_official,
            taxonomy_codes,
            other_identifiers,
            sole_proprietor,
            organization_subpart,
            parent_organization_lbn,
            parent_organization_tin,
        })
    }
    
    /// Parse a taxonomy reference record from CSV row
    fn parse_taxonomy_record(&self, record: &csv::StringRecord) -> Result<TaxonomyReference> {
        let get_field = |index: usize| -> Option<String> {
            record.get(index)
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
        };
        // CSV columns: Code,Grouping,Classification,Specialization,Definition,Notes,Display Name,Section
        Ok(TaxonomyReference {
            code: get_field(0).unwrap_or_default(),
            grouping: get_field(1),
            classification: get_field(2),
            specialization: get_field(3),
            definition: get_field(4),
            notes: get_field(5),
            display_name: get_field(6),
            section: get_field(7),
        })
    }
    
    /// Parse an other name record from CSV row
    fn parse_other_name_record(&self, record: &csv::StringRecord) -> Result<OtherNameRecord> {
        let get_field = |index: usize| -> Option<String> {
            record.get(index)
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
        };
        
        let npi_str = get_field(0).ok_or_else(|| {
            NppesError::DataValidation {
                message: "Missing NPI in other name record".to_string(),
                field: Some("NPI".to_string()),
                value: None,
                context: Default::default(),
            }
        })?;
        let npi = Npi::new(npi_str)?;
        
        Ok(OtherNameRecord {
            npi,
            provider_other_organization_name: get_field(1).unwrap_or_default(),
            provider_other_organization_name_type_code: get_field(2),
        })
    }
    
    /// Parse a practice location record from CSV row
    fn parse_practice_location_record(&self, record: &csv::StringRecord) -> Result<PracticeLocationRecord> {
        let get_field = |index: usize| -> Option<String> {
            record.get(index)
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
        };
        
        let npi_str = get_field(0).ok_or_else(|| {
            NppesError::DataValidation {
                message: "Missing NPI in practice location record".to_string(),
                field: Some("NPI".to_string()),
                value: None,
                context: Default::default(),
            }
        })?;
        let npi = Npi::new(npi_str)?;
        
        let address = Address {
            line_1: get_field(1),
            line_2: get_field(2),
            city: get_field(3),
            postal_code: get_field(5),
            telephone: get_field(7),
            fax: get_field(9),
            state: get_field(4).as_deref().and_then(StateCode::from_code),
            country: get_field(6).as_deref().map(CountryCode::from_code),
        };
        
        Ok(PracticeLocationRecord {
            npi,
            address,
            telephone_extension: get_field(8),
        })
    }
    
    /// Parse an endpoint record from CSV row
    fn parse_endpoint_record(&self, record: &csv::StringRecord) -> Result<EndpointRecord> {
        let get_field = |index: usize| -> Option<String> {
            record.get(index)
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
        };
        
        let npi_str = get_field(0).ok_or_else(|| {
            NppesError::DataValidation {
                message: "Missing NPI in endpoint record".to_string(),
                field: Some("NPI".to_string()),
                value: None,
                context: Default::default(),
            }
        })?;
        let npi = Npi::new(npi_str)?;
        
        let affiliation_address = if get_field(13).is_some() || get_field(14).is_some() {
            Some(Address {
                line_1: get_field(13),
                line_2: get_field(14),
                city: get_field(15),
                postal_code: get_field(18),
                telephone: None,
                fax: None,
                state: get_field(16).as_deref().and_then(StateCode::from_code),
                country: get_field(17).as_deref().map(CountryCode::from_code),
            })
        } else {
            None
        };
        
        Ok(EndpointRecord {
            npi,
            endpoint_type: get_field(1),
            endpoint_type_description: get_field(2),
            endpoint: get_field(3),
            affiliation: get_field(4).map(|s| s == "Y"),
            endpoint_description: get_field(5),
            affiliation_legal_business_name: get_field(6),
            use_code: get_field(7),
            use_description: get_field(8),
            other_use_description: get_field(9),
            content_type: get_field(10),
            content_description: get_field(11),
            other_content_description: get_field(12),
            affiliation_address,
        })
    }
    
    /// Parse a date string in MM/DD/YYYY format
    fn parse_date(&self, date_str: &str) -> Result<NaiveDate> {
        NaiveDate::parse_from_str(date_str, "%m/%d/%Y")
            .map_err(|_| NppesError::date_parse_with_format(date_str, "MM/DD/YYYY"))
    }
}

// Helper functions

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

/// Estimate remaining time based on current progress
fn estimate_remaining_time(current: usize, total: usize, elapsed: Duration) -> Option<Duration> {
    if current == 0 || current >= total {
        return None;
    }
    
    let progress_ratio = current as f64 / total as f64;
    let total_estimated = elapsed.as_secs_f64() / progress_ratio;
    let remaining_secs = total_estimated - elapsed.as_secs_f64();
    
    Some(Duration::from_secs_f64(remaining_secs))
}

/// Get available system memory on Windows
#[cfg(target_os = "windows")]
fn get_available_memory_windows() -> Option<usize> {
    // Windows-specific implementation would go here
    // For now, return None to indicate unknown
    None
}

/// Get available system memory on Unix-like systems
#[cfg(not(target_os = "windows"))]
fn get_available_memory_unix() -> Option<usize> {
    // Unix-specific implementation would go here
    // For now, return None to indicate unknown
    None
} 