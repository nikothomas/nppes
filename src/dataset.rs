/*!
 * Unified dataset API for NPPES data
 * 
 * Provides a builder pattern and unified interface for loading and working with
 * complete NPPES datasets including all reference files.
 */

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use crate::{Result, NppesError, ErrorContext};
use crate::data_types::*;
use crate::reader::NppesReader;
use crate::analytics::NppesAnalytics;

/// Builder for loading a complete NPPES dataset
/// 
/// # Example
/// ```no_run
/// # use nppes::dataset::NppesDatasetBuilder;
/// let dataset = NppesDatasetBuilder::new()
///     .main_data("data/npidata_pfile_20240101-20240107.csv")
///     .taxonomy_reference("data/nucc_taxonomy_240.csv")
///     .other_names("data/othername_pfile_20240101-20240107.csv")
///     .practice_locations("data/pl_pfile_20240101-20240107.csv")
///     .endpoints("data/endpoint_pfile_20240101-20240107.csv")
///     .build()?;
/// # Ok::<(), nppes::NppesError>(())
/// ```
pub struct NppesDatasetBuilder {
    main_data_path: Option<PathBuf>,
    taxonomy_path: Option<PathBuf>,
    other_names_path: Option<PathBuf>,
    practice_locations_path: Option<PathBuf>,
    endpoints_path: Option<PathBuf>,
    skip_invalid_records: bool,
    build_indexes: bool,
    #[cfg(feature = "progress")]
    show_progress: bool,
}

impl Default for NppesDatasetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NppesDatasetBuilder {
    /// Create a new dataset builder
    pub fn new() -> Self {
        Self {
            main_data_path: None,
            taxonomy_path: None,
            other_names_path: None,
            practice_locations_path: None,
            endpoints_path: None,
            skip_invalid_records: false,
            build_indexes: true,
            #[cfg(feature = "progress")]
            show_progress: true,
        }
    }
    
    /// Set the path to the main NPPES data file
    pub fn main_data<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.main_data_path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Set the path to the taxonomy reference file
    pub fn taxonomy_reference<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.taxonomy_path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Set the path to the other names file
    pub fn other_names<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.other_names_path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Set the path to the practice locations file
    pub fn practice_locations<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.practice_locations_path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Set the path to the endpoints file
    pub fn endpoints<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.endpoints_path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Enable or disable skipping invalid records
    pub fn skip_invalid_records(mut self, skip: bool) -> Self {
        self.skip_invalid_records = skip;
        self
    }
    
    /// Enable or disable automatic index building
    pub fn build_indexes(mut self, build: bool) -> Self {
        self.build_indexes = build;
        self
    }
    
    #[cfg(feature = "progress")]
    /// Enable or disable progress bars
    pub fn show_progress(mut self, show: bool) -> Self {
        self.show_progress = show;
        self
    }
    
    /// Build the dataset, loading all specified files
    pub fn build(self) -> Result<NppesDataset> {
        let main_path = self.main_data_path
            .ok_or_else(|| NppesError::Custom {
                message: "Main data file path not specified".to_string(),
                suggestion: Some("Use .main_data() to specify the main NPPES data file".to_string()),
            })?;
        
        println!("Loading NPPES dataset...");
        
        // Create reader with progress support
        let mut reader = NppesReader::new()
            .with_skip_invalid_records(self.skip_invalid_records);
        
        #[cfg(feature = "progress")]
        if self.show_progress {
            reader = reader.with_progress(|info| {
                if info.current_records % 10000 == 0 {
                    println!("Processed {} records...", info.current_records);
                }
            });
        }
        
        // Load main data
        println!("Loading main provider data from: {}", main_path.display());
        let providers = reader.load_main_data(&main_path)?;
        
        // Load taxonomy reference if provided
        let taxonomy_map = if let Some(path) = self.taxonomy_path {
            println!("Loading taxonomy reference from: {}", path.display());
            let taxonomies = reader.load_taxonomy_data(&path)?;
            Some(create_taxonomy_map(taxonomies))
        } else {
            None
        };
        
        // Load other names if provided
        let other_names_map = if let Some(path) = self.other_names_path {
            println!("Loading other names from: {}", path.display());
            let other_names = reader.load_other_name_data(&path)?;
            Some(create_other_names_map(other_names))
        } else {
            None
        };
        
        // Load practice locations if provided
        let practice_locations_map = if let Some(path) = self.practice_locations_path {
            println!("Loading practice locations from: {}", path.display());
            let locations = reader.load_practice_location_data(&path)?;
            Some(create_practice_locations_map(locations))
        } else {
            None
        };
        
        // Load endpoints if provided
        let endpoints_map = if let Some(path) = self.endpoints_path {
            println!("Loading endpoints from: {}", path.display());
            let endpoints = reader.load_endpoint_data(&path)?;
            Some(create_endpoints_map(endpoints))
        } else {
            None
        };
        
        // Build indexes if requested
        let mut dataset = NppesDataset {
            providers,
            taxonomy_map,
            other_names_map,
            practice_locations_map,
            endpoints_map,
            npi_index: None,
            state_index: None,
            taxonomy_index: None,
        };
        
        if self.build_indexes {
            println!("Building indexes...");
            dataset.build_indexes();
        }
        
        println!("Dataset loaded successfully!");
        Ok(dataset)
    }
    
    /// Load a standard dataset from a directory containing all NPPES files
    /// 
    /// Looks for files matching standard NPPES naming patterns in the given directory.
    pub fn from_directory<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        
        if !dir.is_dir() {
            return Err(NppesError::Custom {
                message: format!("'{}' is not a directory", dir.display()),
                suggestion: Some("Provide a directory path containing NPPES data files".to_string()),
            });
        }
        
        let mut builder = Self::new();
        
        // Look for main data file
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            
            if filename.starts_with("npidata_pfile_") && filename.ends_with(".csv") {
                builder = builder.main_data(path);
            } else if filename.starts_with("nucc_taxonomy_") && filename.ends_with(".csv") {
                builder = builder.taxonomy_reference(path);
            } else if filename.starts_with("othername_pfile_") && filename.ends_with(".csv") {
                builder = builder.other_names(path);
            } else if filename.starts_with("pl_pfile_") && filename.ends_with(".csv") {
                builder = builder.practice_locations(path);
            } else if filename.starts_with("endpoint_pfile_") && filename.ends_with(".csv") {
                builder = builder.endpoints(path);
            }
        }
        
        Ok(builder)
    }
}

/// Unified NPPES dataset containing all loaded data and indexes
pub struct NppesDataset {
    /// Main provider records
    pub providers: Vec<NppesRecord>,
    
    /// Taxonomy reference map (code -> reference)
    pub taxonomy_map: Option<HashMap<String, TaxonomyReference>>,
    
    /// Other names map (NPI -> list of other names)
    pub other_names_map: Option<HashMap<Npi, Vec<OtherNameRecord>>>,
    
    /// Practice locations map (NPI -> list of locations)
    pub practice_locations_map: Option<HashMap<Npi, Vec<PracticeLocationRecord>>>,
    
    /// Endpoints map (NPI -> list of endpoints)
    pub endpoints_map: Option<HashMap<Npi, Vec<EndpointRecord>>>,
    
    // Indexes for fast lookup
    npi_index: Option<HashMap<Npi, usize>>,
    state_index: Option<HashMap<String, Vec<usize>>>,
    taxonomy_index: Option<HashMap<String, Vec<usize>>>,
}

impl NppesDataset {
    /// Public constructor for NppesDataset, allowing all fields to be set.
    pub fn new(
        providers: Vec<NppesRecord>,
        taxonomy_map: Option<HashMap<String, TaxonomyReference>>,
        other_names_map: Option<HashMap<Npi, Vec<OtherNameRecord>>>,
        practice_locations_map: Option<HashMap<Npi, Vec<PracticeLocationRecord>>>,
        endpoints_map: Option<HashMap<Npi, Vec<EndpointRecord>>>,
        npi_index: Option<HashMap<Npi, usize>>,
        state_index: Option<HashMap<String, Vec<usize>>>,
        taxonomy_index: Option<HashMap<String, Vec<usize>>>,
    ) -> Self {
        Self {
            providers,
            taxonomy_map,
            other_names_map,
            practice_locations_map,
            endpoints_map,
            npi_index,
            state_index,
            taxonomy_index,
        }
    }
    
    /// Load a standard dataset from a directory
    /// 
    /// Convenience method that looks for standard NPPES files in the given directory.
    /// 
    /// # Example
    /// ```no_run
    /// # use nppes::dataset::NppesDataset;
    /// let dataset = NppesDataset::load_standard("./data")?;
    /// # Ok::<(), nppes::NppesError>(())
    /// ```
    pub fn load_standard<P: AsRef<Path>>(dir: P) -> Result<Self> {
        NppesDatasetBuilder::from_directory(dir)?.build()
    }
    
    /// Get the total number of providers
    pub fn len(&self) -> usize {
        self.providers.len()
    }
    
    /// Check if the dataset is empty
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
    
    /// Build indexes for fast lookups
    pub fn build_indexes(&mut self) {
        #[cfg(feature = "parallel")]
        {
            use rayon::prelude::*;
            
            // Build NPI index
            let npi_index: HashMap<Npi, usize> = self.providers
                .par_iter()
                .enumerate()
                .map(|(idx, provider)| (provider.npi.clone(), idx))
                .collect();
            
            // Build state index
            let mut state_index: HashMap<String, Vec<usize>> = HashMap::new();
            for (idx, provider) in self.providers.iter().enumerate() {
                if let Some(state) = &provider.mailing_address.state {
                    state_index.entry(state.clone())
                        .or_default()
                        .push(idx);
                }
            }
            
            // Build taxonomy index
            let mut taxonomy_index: HashMap<String, Vec<usize>> = HashMap::new();
            for (idx, provider) in self.providers.iter().enumerate() {
                for taxonomy in &provider.taxonomy_codes {
                    taxonomy_index.entry(taxonomy.code.clone())
                        .or_default()
                        .push(idx);
                }
            }
            
            self.npi_index = Some(npi_index);
            self.state_index = Some(state_index);
            self.taxonomy_index = Some(taxonomy_index);
        }
        
        #[cfg(not(feature = "parallel"))]
        {
            // Sequential index building
            let mut npi_index = HashMap::new();
            let mut state_index: HashMap<String, Vec<usize>> = HashMap::new();
            let mut taxonomy_index: HashMap<String, Vec<usize>> = HashMap::new();
            
            for (idx, provider) in self.providers.iter().enumerate() {
                npi_index.insert(provider.npi.clone(), idx);
                
                if let Some(state) = &provider.mailing_address.state {
                    state_index.entry(state.clone())
                        .or_default()
                        .push(idx);
                }
                
                for taxonomy in &provider.taxonomy_codes {
                    taxonomy_index.entry(taxonomy.code.clone())
                        .or_default()
                        .push(idx);
                }
            }
            
            self.npi_index = Some(npi_index);
            self.state_index = Some(state_index);
            self.taxonomy_index = Some(taxonomy_index);
        }
    }
    
    /// Get a provider by NPI (O(1) if indexed)
    pub fn get_by_npi(&self, npi: &Npi) -> Option<&NppesRecord> {
        if let Some(index) = &self.npi_index {
            index.get(npi).and_then(|&idx| self.providers.get(idx))
        } else {
            self.providers.iter().find(|p| &p.npi == npi)
        }
    }
    
    /// Get all providers in a state (fast if indexed)
    pub fn get_by_state(&self, state: &str) -> Vec<&NppesRecord> {
        if let Some(index) = &self.state_index {
            index.get(state)
                .map(|indices| {
                    indices.iter()
                        .filter_map(|&idx| self.providers.get(idx))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            self.providers.iter()
                .filter(|p| {
                    p.mailing_address.state.as_ref()
                        .map(|s| s.eq_ignore_ascii_case(state))
                        .unwrap_or(false)
                })
                .collect()
        }
    }
    
    /// Get all providers with a specific taxonomy code (fast if indexed)
    pub fn get_by_taxonomy(&self, taxonomy_code: &str) -> Vec<&NppesRecord> {
        if let Some(index) = &self.taxonomy_index {
            index.get(taxonomy_code)
                .map(|indices| {
                    indices.iter()
                        .filter_map(|&idx| self.providers.get(idx))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            self.providers.iter()
                .filter(|p| {
                    p.taxonomy_codes.iter().any(|t| t.code == taxonomy_code)
                })
                .collect()
        }
    }
    
    /// Get taxonomy description for a code
    pub fn get_taxonomy_description(&self, code: &str) -> Option<&TaxonomyReference> {
        self.taxonomy_map.as_ref()?.get(code)
    }
    
    /// Get other names for an NPI
    pub fn get_other_names(&self, npi: &Npi) -> Option<&Vec<OtherNameRecord>> {
        self.other_names_map.as_ref()?.get(npi)
    }
    
    /// Get practice locations for an NPI
    pub fn get_practice_locations(&self, npi: &Npi) -> Option<&Vec<PracticeLocationRecord>> {
        self.practice_locations_map.as_ref()?.get(npi)
    }
    
    /// Get endpoints for an NPI
    pub fn get_endpoints(&self, npi: &Npi) -> Option<&Vec<EndpointRecord>> {
        self.endpoints_map.as_ref()?.get(npi)
    }
    
    /// Create an analytics engine for this dataset
    pub fn analytics(&self) -> NppesAnalytics {
        NppesAnalytics::new(&self.providers)
    }
    
    /// Create a query builder for this dataset
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new(self)
    }
    
    /// Get dataset statistics
    pub fn statistics(&self) -> DatasetStatistics {
        DatasetStatistics::from_dataset(self)
    }
}

/// Query builder for NPPES dataset
pub struct QueryBuilder<'a> {
    dataset: &'a NppesDataset,
    filters: Vec<Box<dyn Fn(&NppesRecord) -> bool + Send + Sync + 'a>>,
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder
    pub fn new(dataset: &'a NppesDataset) -> Self {
        Self {
            dataset,
            filters: Vec::new(),
        }
    }
    
    /// Filter by state
    pub fn state(mut self, state: &'a str) -> Self {
        self.filters.push(Box::new(move |p| {
            p.mailing_address.state.as_ref()
                .map(|s| s.eq_ignore_ascii_case(state))
                .unwrap_or(false)
        }));
        self
    }
    
    /// Filter by multiple states
    pub fn state_in(mut self, states: &'a [&str]) -> Self {
        self.filters.push(Box::new(move |p| {
            p.mailing_address.state.as_ref()
                .map(|s| states.iter().any(|state| s.eq_ignore_ascii_case(state)))
                .unwrap_or(false)
        }));
        self
    }
    
    /// Filter by specialty (taxonomy display name)
    pub fn specialty(mut self, specialty: &'a str) -> Self {
        let specialty_lower = specialty.to_lowercase();
        self.filters.push(Box::new(move |p| {
            p.taxonomy_codes.iter().any(|t| {
                if let Some(taxonomy_ref) = self.dataset.get_taxonomy_description(&t.code) {
                    taxonomy_ref.display_name.as_ref()
                        .map(|name| name.to_lowercase().contains(&specialty_lower))
                        .unwrap_or(false)
                } else {
                    false
                }
            })
        }));
        self
    }
    
    /// Filter by entity type
    pub fn entity_type(mut self, entity_type: EntityType) -> Self {
        self.filters.push(Box::new(move |p| p.entity_type == entity_type));
        self
    }
    
    /// Filter by active status
    pub fn active_only(mut self) -> Self {
        self.filters.push(Box::new(|p| p.is_active()));
        self
    }
    
    /// Execute the query and return matching providers
    pub fn execute(self) -> Vec<&'a NppesRecord> {
        #[cfg(feature = "parallel")]
        {
            use rayon::prelude::*;
            self.dataset.providers.par_iter()
                .filter(|provider| {
                    self.filters.iter().all(|filter| filter(provider))
                })
                .collect()
        }
        
        #[cfg(not(feature = "parallel"))]
        {
            self.dataset.providers.iter()
                .filter(|provider| {
                    self.filters.iter().all(|filter| filter(provider))
                })
                .collect()
        }
    }
    
    /// Execute the query and return count only
    pub fn count(self) -> usize {
        self.execute().len()
    }
    
    /// Execute the query with a limit
    pub fn limit(self, limit: usize) -> Vec<&'a NppesRecord> {
        let mut results = Vec::new();
        for provider in &self.dataset.providers {
            if self.filters.iter().all(|filter| filter(provider)) {
                results.push(provider);
                if results.len() >= limit {
                    break;
                }
            }
        }
        results
    }
}

/// Dataset statistics
#[derive(Debug, Clone)]
pub struct DatasetStatistics {
    pub total_providers: usize,
    pub individual_providers: usize,
    pub organization_providers: usize,
    pub active_providers: usize,
    pub inactive_providers: usize,
    pub states_represented: usize,
    pub unique_taxonomy_codes: usize,
    pub providers_with_other_names: usize,
    pub providers_with_practice_locations: usize,
    pub providers_with_endpoints: usize,
}

impl DatasetStatistics {
    /// Calculate statistics from a dataset
    pub fn from_dataset(dataset: &NppesDataset) -> Self {
        let mut stats = Self {
            total_providers: dataset.providers.len(),
            individual_providers: 0,
            organization_providers: 0,
            active_providers: 0,
            inactive_providers: 0,
            states_represented: 0,
            unique_taxonomy_codes: 0,
            providers_with_other_names: 0,
            providers_with_practice_locations: 0,
            providers_with_endpoints: 0,
        };
        
        let mut states = std::collections::HashSet::new();
        let mut taxonomies = std::collections::HashSet::new();
        
        for provider in &dataset.providers {
            match provider.entity_type {
                EntityType::Individual => stats.individual_providers += 1,
                EntityType::Organization => stats.organization_providers += 1,
            }
            
            if provider.is_active() {
                stats.active_providers += 1;
            } else {
                stats.inactive_providers += 1;
            }
            
            if let Some(state) = &provider.mailing_address.state {
                states.insert(state.clone());
            }
            
            for taxonomy in &provider.taxonomy_codes {
                taxonomies.insert(taxonomy.code.clone());
            }
        }
        
        stats.states_represented = states.len();
        stats.unique_taxonomy_codes = taxonomies.len();
        
        if let Some(other_names) = &dataset.other_names_map {
            stats.providers_with_other_names = other_names.len();
        }
        
        if let Some(locations) = &dataset.practice_locations_map {
            stats.providers_with_practice_locations = locations.len();
        }
        
        if let Some(endpoints) = &dataset.endpoints_map {
            stats.providers_with_endpoints = endpoints.len();
        }
        
        stats
    }
    
    /// Print a formatted summary of the statistics
    pub fn print_summary(&self) {
        println!("=== NPPES Dataset Statistics ===");
        println!("Total Providers: {}", self.total_providers);
        println!("  Individual: {} ({:.1}%)", 
            self.individual_providers,
            (self.individual_providers as f64 / self.total_providers as f64) * 100.0
        );
        println!("  Organization: {} ({:.1}%)",
            self.organization_providers,
            (self.organization_providers as f64 / self.total_providers as f64) * 100.0
        );
        println!("Active Providers: {} ({:.1}%)",
            self.active_providers,
            (self.active_providers as f64 / self.total_providers as f64) * 100.0
        );
        println!("States Represented: {}", self.states_represented);
        println!("Unique Taxonomy Codes: {}", self.unique_taxonomy_codes);
        
        if self.providers_with_other_names > 0 {
            println!("Providers with Other Names: {}", self.providers_with_other_names);
        }
        if self.providers_with_practice_locations > 0 {
            println!("Providers with Practice Locations: {}", self.providers_with_practice_locations);
        }
        if self.providers_with_endpoints > 0 {
            println!("Providers with Endpoints: {}", self.providers_with_endpoints);
        }
    }
}

// Helper functions to create lookup maps
fn create_taxonomy_map(records: Vec<TaxonomyReference>) -> HashMap<String, TaxonomyReference> {
    records.into_iter()
        .map(|r| (r.code.clone(), r))
        .collect()
}

fn create_other_names_map(records: Vec<OtherNameRecord>) -> HashMap<Npi, Vec<OtherNameRecord>> {
    let mut map = HashMap::new();
    for record in records {
        map.entry(record.npi.clone())
            .or_insert_with(Vec::new)
            .push(record);
    }
    map
}

fn create_practice_locations_map(records: Vec<PracticeLocationRecord>) -> HashMap<Npi, Vec<PracticeLocationRecord>> {
    let mut map = HashMap::new();
    for record in records {
        map.entry(record.npi.clone())
            .or_insert_with(Vec::new)
            .push(record);
    }
    map
}

fn create_endpoints_map(records: Vec<EndpointRecord>) -> HashMap<Npi, Vec<EndpointRecord>> {
    let mut map = HashMap::new();
    for record in records {
        map.entry(record.npi.clone())
            .or_insert_with(Vec::new)
            .push(record);
    }
    map
} 