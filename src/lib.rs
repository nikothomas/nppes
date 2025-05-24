/*!
 * # NPPES (National Plan and Provider Enumeration System) Data Library
 * 
 * A comprehensive Rust library for working with NPPES healthcare provider data.
 * 
 * ## Features
 * 
 * - 🚀 **High Performance**: Efficient parsing of 9.9GB+ datasets with progress tracking
 * - 🔧 **Easy to Use**: Simple builder pattern for loading complete datasets
 * - 📊 **Rich Analytics**: Built-in querying and statistical analysis
 * - 💾 **Multiple Export Formats**: JSON, CSV, SQL, and more
 * - 🔍 **Fast Lookups**: Automatic indexing for O(1) provider lookups
 * - 🧩 **Modular Design**: Load only the data you need
 * - 🛡️ **Type Safe**: Strongly typed data structures with validation
 * 
 * ## Quick Start
 * 
 * ```no_run
 * use nppes::prelude::*;
 * 
 * # fn main() -> Result<()> {
 * // Simple one-liner to load all NPPES data from a directory
 * let dataset = NppesDataset::load_standard("./data")?;
 * 
 * // Find providers
 * let ca_cardiologists = dataset
 *     .query()
 *     .state("CA")
 *     .specialty("Cardiology")
 *     .active_only()
 *     .execute();
 * 
 * println!("Found {} cardiologists in California", ca_cardiologists.len());
 * 
 * // Export results
 * dataset.export_subset(
 *     "ca_cardiologists.json",
 *     |p| p.mailing_address.state.as_deref() == Some("CA"),
 *     ExportFormat::Json
 * )?;
 * # Ok(())
 * # }
 * ```
 * 
 * ## Loading Data
 * 
 * ### Using the Builder Pattern
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # fn main() -> Result<()> {
 * let dataset = NppesDatasetBuilder::new()
 *     .main_data("data/npidata_pfile_20240101-20240107.csv")
 *     .taxonomy_reference("data/nucc_taxonomy_240.csv")
 *     .other_names("data/othername_pfile_20240101-20240107.csv")
 *     .skip_invalid_records(true)
 *     .build()?;
 * # Ok(())
 * # }
 * ```
 * 
 * ### Memory Estimation
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # fn main() -> Result<()> {
 * // Check memory requirements before loading
 * let estimate = NppesReader::estimate_memory_usage("data/npidata_pfile.csv")?;
 * println!("Estimated memory usage: {}", estimate.estimated_memory_human);
 * # Ok(())
 * # }
 * ```
 * 
 * ## Querying Data
 * 
 * ### Find Providers by Criteria
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # fn main() -> Result<()> {
 * # let dataset = NppesDataset::load_standard("./data")?;
 * // Find all active primary care physicians in New York
 * let ny_pcps = dataset
 *     .query()
 *     .state("NY")
 *     .entity_type(EntityType::Individual)
 *     .specialty("Primary Care")
 *     .active_only()
 *     .execute();
 * 
 * // Get providers by NPI (O(1) lookup if indexed)
 * if let Some(provider) = dataset.get_by_npi(&Npi::new("1234567890".to_string())?) {
 *     println!("Provider: {}", provider.display_name());
 * }
 * # Ok(())
 * # }
 * ```
 * 
 * ### Statistical Analysis
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # fn main() -> Result<()> {
 * # let dataset = NppesDataset::load_standard("./data")?;
 * // Get dataset statistics
 * let stats = dataset.statistics();
 * stats.print_summary();
 * 
 * // Use analytics engine for advanced queries
 * let analytics = dataset.analytics();
 * let top_states = analytics.top_states_by_provider_count(10);
 * # Ok(())
 * # }
 * ```
 * 
 * ## Exporting Data
 * 
 * ### Export to Different Formats
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # use nppes::export::SqlDialect;
 * # fn main() -> Result<()> {
 * # let dataset = NppesDataset::load_standard("./data")?;
 * // Export to JSON
 * dataset.export_json("providers.json")?;
 * 
 * // Export to JSON Lines (streaming format)
 * dataset.export_json_lines("providers.jsonl")?;
 * 
 * // Export to normalized CSV files
 * dataset.export_csv("providers.csv")?;
 * 
 * // Export to SQL
 * dataset.export_sql("providers.sql", SqlDialect::PostgreSQL)?;
 * 
 * // Export to Parquet (if enabled)
 * #[cfg(feature = "arrow-export")]
 * dataset.export_parquet("providers.parquet")?;
 * 
 * // Export filtered subset
 * dataset.export_subset(
 *     "texas_organizations.json",
 *     |p| p.entity_type == EntityType::Organization && 
 *         p.mailing_address.state.as_deref() == Some("TX"),
 *     ExportFormat::Json
 * )?;
 * # Ok(())
 * # }
 * ```
 * 
 * ## Configuration
 * 
 * ### Using Configuration
 * 
 * ```no_run
 * # use nppes::prelude::*;
 * # use nppes::config::{NppesConfig, ValidationLevel};
 * # fn main() -> Result<()> {
 * // Use a custom configuration
 * let config = NppesConfig::performance();
 * nppes::config::set_global_config(config);
 * 
 * // Or build your own
 * let config = ConfigBuilder::new()
 *     .progress_bar(false)
 *     .validation_level(ValidationLevel::Basic)
 *     .skip_invalid_records(true)
 *     .build();
 * # Ok(())
 * # }
 * ```
 * 
 * ## Performance Tips
 * 
 * 1. **Use Indexes**: The dataset automatically builds indexes for fast lookups
 * 2. **Enable Parallel Processing**: Use the `parallel` feature for faster operations
 * 3. **Skip Invalid Records**: Set `skip_invalid_records(true)` for resilient parsing
 * 4. **Memory Estimation**: Check memory requirements before loading large files
 * 5. **Progress Tracking**: Enable progress bars for long operations
 * 
 * ## NPPES Data Files
 * 
 * The library supports all NPPES file types:
 * 
 * - **Main Data File**: `npidata_pfile_YYYYMMDD-YYYYMMDD.csv` (~9.9GB)
 * - **Other Names**: `othername_pfile_YYYYMMDD-YYYYMMDD.csv`
 * - **Practice Locations**: `pl_pfile_YYYYMMDD-YYYYMMDD.csv`
 * - **Endpoints**: `endpoint_pfile_YYYYMMDD-YYYYMMDD.csv`
 * - **Taxonomy Reference**: `nucc_taxonomy_XXX.csv`
 * 
 * Download files from: https://download.cms.gov/nppes/NPI_Files.html
 */

// Re-export error types from root
pub use error::{NppesError, Result, ErrorContext, ExportFormat};

// Public modules
pub mod data_types;
pub mod reader;
pub mod schema;
pub mod error;
pub mod analytics;
pub mod dataset;
pub mod export;
pub mod config;

/// Prelude module for convenient imports
/// 
/// Import everything you need with:
/// ```
/// use nppes::prelude::*;
/// ```
pub mod prelude {
    pub use crate::data_types::*;
    pub use crate::reader::{NppesReader, ProgressInfo, MemoryEstimate};
    pub use crate::schema::*;
    pub use crate::error::{NppesError, Result};
    pub use crate::analytics::{NppesAnalytics, DatasetStats};
    pub use crate::dataset::{NppesDataset, NppesDatasetBuilder, DatasetStatistics};
    pub use crate::export::{NppesExporter, JsonExporter, CsvExporter, SqlExporter};
    #[cfg(feature = "arrow-export")]
    pub use crate::export::ParquetExporter;
    pub use crate::config::{ConfigBuilder, ValidationLevel};
    pub use crate::ExportFormat;
}

/// NPPES data constants
pub mod constants {
    /// Maximum number of healthcare taxonomy codes per provider
    pub const MAX_TAXONOMY_CODES: usize = 15;
    
    /// Maximum number of other provider identifiers per provider
    pub const MAX_OTHER_IDENTIFIERS: usize = 50;
    
    /// Entity type code for Individual providers
    pub const ENTITY_TYPE_INDIVIDUAL: &str = "1";
    
    /// Entity type code for Organization providers
    pub const ENTITY_TYPE_ORGANIZATION: &str = "2";
    
    /// NPPES file naming patterns
    pub const MAIN_DATA_FILE_PATTERN: &str = "npidata_pfile_*-*.csv";
    pub const OTHER_NAME_FILE_PATTERN: &str = "othername_pfile_*-*.csv";
    pub const PRACTICE_LOCATION_FILE_PATTERN: &str = "pl_pfile_*-*.csv";
    pub const ENDPOINT_FILE_PATTERN: &str = "endpoint_pfile_*-*.csv";
    pub const TAXONOMY_FILE_PATTERN: &str = "nucc_taxonomy_*.csv";
}

/// Common recipes and utility functions
pub mod cookbook {
    use crate::prelude::*;
    use std::collections::HashMap;
    
    /// Find all providers within a specific specialty in a state
    /// 
    /// # Example
    /// ```no_run
    /// # use nppes::prelude::*;
    /// # use nppes::cookbook::find_specialists_in_state;
    /// # fn main() -> Result<()> {
    /// # let dataset = NppesDataset::load_standard("./data")?;
    /// let ca_surgeons = find_specialists_in_state(&dataset, "Surgery", "CA");
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_specialists_in_state<'a>(
        dataset: &'a NppesDataset, 
        specialty: &'a str, 
        state: &'a str
    ) -> Vec<&'a NppesRecord> {
        dataset.query()
            .state(state)
            .specialty(specialty)
            .active_only()
            .execute()
    }
    
    /// Get provider distribution by state
    /// 
    /// Returns a map of state codes to provider counts
    pub fn provider_distribution_by_state(dataset: &NppesDataset) -> HashMap<String, usize> {
        let mut distribution = HashMap::new();
        
        for provider in &dataset.providers {
            if let Some(state) = &provider.mailing_address.state {
                *distribution.entry(state.clone()).or_insert(0) += 1;
            }
        }
        
        distribution
    }
    
    /// Find providers by partial name match
    /// 
    /// Case-insensitive search across organization and individual names
    pub fn find_by_partial_name<'a>(
        dataset: &'a NppesDataset,
        name_query: &str
    ) -> Vec<&'a NppesRecord> {
        let query_lower = name_query.to_lowercase();
        
        dataset.providers.iter()
            .filter(|p| {
                let display_name = p.display_name().to_lowercase();
                display_name.contains(&query_lower)
            })
            .collect()
    }
    
    /// Get all unique taxonomy codes with descriptions
    /// 
    /// Returns a sorted list of (code, description) pairs
    pub fn get_all_specialties(dataset: &NppesDataset) -> Vec<(String, String)> {
        let mut specialties = std::collections::HashMap::new();
        
        for provider in &dataset.providers {
            for taxonomy in &provider.taxonomy_codes {
                if let Some(desc) = dataset.get_taxonomy_description(&taxonomy.code) {
                    if let Some(display_name) = &desc.display_name {
                        specialties.insert(taxonomy.code.clone(), display_name.clone());
                    }
                }
            }
        }
        
        let mut result: Vec<_> = specialties.into_iter().collect();
        result.sort_by(|a, b| a.1.cmp(&b.1));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::{Npi, EntityType};
    
    #[test]
    fn test_npi_validation() {
        assert!(Npi::new("1234567890".to_string()).is_ok());
        assert!(Npi::new("123".to_string()).is_err());
        assert!(Npi::new("12345678AB".to_string()).is_err());
    }
    
    #[test]
    fn test_entity_type() {
        assert_eq!(EntityType::from_code("1").unwrap(), EntityType::Individual);
        assert_eq!(EntityType::from_code("2").unwrap(), EntityType::Organization);
        assert!(EntityType::from_code("3").is_err());
    }
} 