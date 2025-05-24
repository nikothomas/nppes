/*!
 * Unified dataset API for NPPES data
 * 
 * Provides a builder pattern and unified interface for loading and working with
 * complete NPPES datasets including all reference files.
 */

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use crate::{Result, NppesError};
use crate::data_types::*;
use crate::reader::NppesReader;
use crate::analytics::NppesAnalytics;

#[cfg(feature = "download")]
use crate::download::{NppesDownloader, DownloadConfig, ExtractedFiles};

/// Data source - either a local file path or a URL
#[derive(Debug, Clone)]
pub enum DataSource {
    /// Local file path
    File(PathBuf),
    /// URL to download from
    Url(String),
}

impl DataSource {
    /// Create a DataSource from a string (auto-detects URL vs file path)
    pub fn from_str(s: &str) -> Self {
        if s.starts_with("http://") || s.starts_with("https://") {
            DataSource::Url(s.to_string())
        } else {
            DataSource::File(PathBuf::from(s))
        }
    }
    
    /// Create a DataSource from a path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        DataSource::File(path.as_ref().to_path_buf())
    }
}

impl From<&str> for DataSource {
    fn from(s: &str) -> Self {
        DataSource::from_str(s)
    }
}

impl From<String> for DataSource {
    fn from(s: String) -> Self {
        DataSource::from_str(&s)
    }
}

impl From<PathBuf> for DataSource {
    fn from(path: PathBuf) -> Self {
        DataSource::File(path)
    }
}

impl From<&Path> for DataSource {
    fn from(path: &Path) -> Self {
        DataSource::File(path.to_path_buf())
    }
}

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
/// 
/// # Example with URL
/// ```no_run
/// # use nppes::dataset::NppesDatasetBuilder;
/// # #[cfg(feature = "download")]
/// # tokio_test::block_on(async {
/// let dataset = NppesDatasetBuilder::new()
///     .from_url("https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2025_V2.zip")
///     .build_async().await?;
/// # Ok::<(), nppes::NppesError>(())
/// # });
/// ```
pub struct NppesDatasetBuilder {
    main_data_source: Option<DataSource>,
    taxonomy_source: Option<DataSource>,
    other_names_source: Option<DataSource>,
    practice_locations_source: Option<DataSource>,
    endpoints_source: Option<DataSource>,
    skip_invalid_records: bool,
    build_indexes: bool,
    #[cfg(feature = "progress")]
    show_progress: bool,
    #[cfg(feature = "download")]
    download_config: Option<DownloadConfig>,
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
            main_data_source: None,
            taxonomy_source: None,
            other_names_source: None,
            practice_locations_source: None,
            endpoints_source: None,
            skip_invalid_records: false,
            build_indexes: true,
            #[cfg(feature = "progress")]
            show_progress: true,
            #[cfg(feature = "download")]
            download_config: None,
        }
    }
    
    /// Set the path or URL to the main NPPES data file
    pub fn main_data<S: Into<DataSource>>(mut self, source: S) -> Self {
        self.main_data_source = Some(source.into());
        self
    }
    
    /// Set the path or URL to the taxonomy reference file
    pub fn taxonomy_reference<S: Into<DataSource>>(mut self, source: S) -> Self {
        self.taxonomy_source = Some(source.into());
        self
    }
    
    /// Set the path or URL to the other names file
    pub fn other_names<S: Into<DataSource>>(mut self, source: S) -> Self {
        self.other_names_source = Some(source.into());
        self
    }
    
    /// Set the path or URL to the practice locations file
    pub fn practice_locations<S: Into<DataSource>>(mut self, source: S) -> Self {
        self.practice_locations_source = Some(source.into());
        self
    }
    
    /// Set the path or URL to the endpoints file
    pub fn endpoints<S: Into<DataSource>>(mut self, source: S) -> Self {
        self.endpoints_source = Some(source.into());
        self
    }
    
    /// Load data from a URL (ZIP file containing NPPES data)
    #[cfg(feature = "download")]
    pub fn from_url<S: Into<String>>(mut self, url: S) -> Self {
        self.main_data_source = Some(DataSource::Url(url.into()));
        self
    }
    
    /// Set download configuration
    #[cfg(feature = "download")]
    pub fn with_download_config(mut self, config: DownloadConfig) -> Self {
        self.download_config = Some(config);
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
    
    /// Build the dataset, loading all specified files (synchronous version)
    pub fn build(self) -> Result<NppesDataset> {
        #[cfg(feature = "download")]
        {
            let rt = tokio::runtime::Runtime::new().map_err(|e| NppesError::Custom {
                message: format!("Failed to create async runtime: {}", e),
                suggestion: Some("Use build_async() if you're already in an async context".to_string()),
            })?;
            
            rt.block_on(self.build_async())
        }
        
        #[cfg(not(feature = "download"))]
        {
            // For non-download builds, we need to handle this synchronously
            // Since we can't use async, we'll need to manually handle the sync case
            let main_source = self.main_data_source
                .ok_or_else(|| NppesError::Custom {
                    message: "Main data source not specified".to_string(),
                    suggestion: Some("Use .main_data() to specify the main NPPES data source".to_string()),
                })?;
            
            // Only file sources are supported without download feature
            match main_source {
                DataSource::File(path) => {
                    let resolved_sources = ResolvedSources {
                        main_data_path: path,
                        taxonomy_path: self.taxonomy_source.as_ref().and_then(|s| match s {
                            DataSource::File(p) => Some(p.clone()),
                            DataSource::Url(_) => None,
                        }),
                        other_names_path: self.other_names_source.as_ref().and_then(|s| match s {
                            DataSource::File(p) => Some(p.clone()),
                            DataSource::Url(_) => None,
                        }),
                        practice_locations_path: self.practice_locations_source.as_ref().and_then(|s| match s {
                            DataSource::File(p) => Some(p.clone()),
                            DataSource::Url(_) => None,
                        }),
                        endpoints_path: self.endpoints_source.as_ref().and_then(|s| match s {
                            DataSource::File(p) => Some(p.clone()),
                            DataSource::Url(_) => None,
                        }),
                    };
                    
                    Self::build_from_resolved_sources_static(
                        resolved_sources,
                        self.skip_invalid_records,
                        self.build_indexes,
                        #[cfg(feature = "progress")]
                        self.show_progress,
                    )
                }
                DataSource::Url(_) => {
                    Err(NppesError::feature_required("download"))
                }
            }
        }
    }
    
    /// Build the dataset from internet sources (async version)
    pub async fn build_async(self) -> Result<NppesDataset> {
        let main_source = self.main_data_source
            .ok_or_else(|| NppesError::Custom {
                message: "Main data source not specified".to_string(),
                suggestion: Some("Use .main_data() or .from_url() to specify the main NPPES data source".to_string()),
            })?;
        
        println!("Loading NPPES dataset...");
        
        // Extract all fields we need before moving them
        let taxonomy_source = self.taxonomy_source;
        let other_names_source = self.other_names_source;
        let practice_locations_source = self.practice_locations_source;
        let endpoints_source = self.endpoints_source;
        let skip_invalid_records = self.skip_invalid_records;
        let build_indexes = self.build_indexes;
        #[cfg(feature = "progress")]
        let show_progress = self.show_progress;
        #[cfg(feature = "download")]
        let download_config = self.download_config;
        
        let resolved_sources = Self::resolve_sources_static(
            main_source,
            taxonomy_source,
            other_names_source,
            practice_locations_source,
            endpoints_source,
            #[cfg(feature = "download")]
            download_config,
            #[cfg(not(feature = "download"))]
            None,
        ).await?;
        
        Self::build_from_resolved_sources_static(
            resolved_sources,
            skip_invalid_records,
            build_indexes,
            #[cfg(feature = "progress")]
            show_progress,
        )
    }
    
    /// Build dataset from resolved sources (static version)
    fn build_from_resolved_sources_static(
        resolved_sources: ResolvedSources,
        skip_invalid_records: bool,
        build_indexes: bool,
        #[cfg(feature = "progress")]
        show_progress: bool,
    ) -> Result<NppesDataset> {
        // Create reader with progress support
        let mut reader = NppesReader::new()
            .with_skip_invalid_records(skip_invalid_records);
        
        #[cfg(feature = "progress")]
        if show_progress {
            // When showing progress, disable the default progress bar from the reader
            // and don't use the callback that prints to stdout
            reader = reader.with_progress_bar(false);
        }
        
        // Load main data
        #[cfg(feature = "progress")]
        if !show_progress {
            println!("Loading main provider data from: {}", resolved_sources.main_data_path.display());
        }
        
        #[cfg(not(feature = "progress"))]
        println!("Loading main provider data from: {}", resolved_sources.main_data_path.display());
        
        let providers = reader.load_main_data(&resolved_sources.main_data_path)?;
        
        // Load other data files
        let taxonomy_map = if let Some(path) = resolved_sources.taxonomy_path {
            #[cfg(feature = "progress")]
            if !show_progress {
                println!("Loading taxonomy reference from: {}", path.display());
            }
            
            #[cfg(not(feature = "progress"))]
            println!("Loading taxonomy reference from: {}", path.display());
            
            let taxonomies = reader.load_taxonomy_data(&path)?;
            Some(create_taxonomy_map(taxonomies))
        } else {
            None
        };
        
        let other_names_map = if let Some(path) = resolved_sources.other_names_path {
            #[cfg(feature = "progress")]
            if !show_progress {
                println!("Loading other names from: {}", path.display());
            }
            
            #[cfg(not(feature = "progress"))]
            println!("Loading other names from: {}", path.display());
            
            let other_names = reader.load_other_name_data(&path)?;
            Some(create_other_names_map(other_names))
        } else {
            None
        };
        
        let practice_locations_map = if let Some(path) = resolved_sources.practice_locations_path {
            #[cfg(feature = "progress")]
            if !show_progress {
                println!("Loading practice locations from: {}", path.display());
            }
            
            #[cfg(not(feature = "progress"))]
            println!("Loading practice locations from: {}", path.display());
            
            let locations = reader.load_practice_location_data(&path)?;
            Some(create_practice_locations_map(locations))
        } else {
            None
        };
        
        let endpoints_map = if let Some(path) = resolved_sources.endpoints_path {
            #[cfg(feature = "progress")]
            if !show_progress {
                println!("Loading endpoints from: {}", path.display());
            }
            
            #[cfg(not(feature = "progress"))]
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
        
        if build_indexes {
            #[cfg(feature = "progress")]
            if !show_progress {
                println!("Building indexes...");
            }
            
            #[cfg(not(feature = "progress"))]
            println!("Building indexes...");
            
            dataset.build_indexes();
        }
        
        #[cfg(feature = "progress")]
        if !show_progress {
            println!("Dataset loaded successfully!");
        }
        
        #[cfg(not(feature = "progress"))]
        println!("Dataset loaded successfully!");
        
        Ok(dataset)
    }
    
    /// Resolve data sources (download URLs if needed) - static version
    async fn resolve_sources_static(
        main_source: DataSource,
        taxonomy_source: Option<DataSource>,
        other_names_source: Option<DataSource>,
        practice_locations_source: Option<DataSource>,
        endpoints_source: Option<DataSource>,
        #[cfg(feature = "download")]
        download_config: Option<DownloadConfig>,
        #[cfg(not(feature = "download"))]
        _download_config: Option<()>,
    ) -> Result<ResolvedSources> {
        match main_source {
            DataSource::File(path) => {
                // All local files - just return paths
                Ok(ResolvedSources {
                    main_data_path: path,
                    taxonomy_path: taxonomy_source.and_then(|s| match s {
                        DataSource::File(p) => Some(p),
                        DataSource::Url(_) => None, // Handle mixed sources separately if needed
                    }),
                    other_names_path: other_names_source.and_then(|s| match s {
                        DataSource::File(p) => Some(p),
                        DataSource::Url(_) => None,
                    }),
                    practice_locations_path: practice_locations_source.and_then(|s| match s {
                        DataSource::File(p) => Some(p),
                        DataSource::Url(_) => None,
                    }),
                    endpoints_path: endpoints_source.and_then(|s| match s {
                        DataSource::File(p) => Some(p),
                        DataSource::Url(_) => None,
                    }),
                })
            }
            DataSource::Url(url) => {
                #[cfg(feature = "download")]
                {
                    // Download and extract
                    let config = download_config.unwrap_or_default();
                    let mut downloader = NppesDownloader::with_config(config);
                    
                    let extracted = downloader.download_and_extract_zip(&url, None).await?;
                    
                    println!("{}", extracted.summary());
                    
                    if !extracted.has_main_data() {
                        return Err(NppesError::Custom {
                            message: "No main NPPES data file found in downloaded archive".to_string(),
                            suggestion: Some("Check that the URL points to a valid NPPES data archive".to_string()),
                        });
                    }
                    
                    Ok(ResolvedSources {
                        main_data_path: extracted.main_data_file.unwrap(),
                        taxonomy_path: extracted.taxonomy_file,
                        other_names_path: extracted.other_names_file,
                        practice_locations_path: extracted.practice_locations_file,
                        endpoints_path: extracted.endpoints_file,
                    })
                }
                #[cfg(not(feature = "download"))]
                {
                    Err(NppesError::feature_required("download"))
                }
            }
        }
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
    
    /// Download the latest NPPES data and build dataset
    #[cfg(feature = "download")]
    pub async fn download_latest() -> Result<NppesDataset> {
        let mut downloader = NppesDownloader::new();
        let extracted = downloader.download_latest_nppes().await?;
        
        if !extracted.has_main_data() {
            return Err(NppesError::Custom {
                message: "No main NPPES data file found in latest download".to_string(),
                suggestion: Some("The CMS website structure may have changed".to_string()),
            });
        }
        
        NppesDatasetBuilder::new()
            .main_data(extracted.main_data_file.as_ref().unwrap().as_path())
            .taxonomy_reference(extracted.taxonomy_file.as_ref().unwrap().as_path())
            .other_names(extracted.other_names_file.as_deref().unwrap_or(&PathBuf::new()))
            .practice_locations(extracted.practice_locations_file.as_deref().unwrap_or(&PathBuf::new()))
            .endpoints(extracted.endpoints_file.as_deref().unwrap_or(&PathBuf::new()))
            .build()
    }
}

/// Resolved file paths after downloading
struct ResolvedSources {
    main_data_path: PathBuf,
    taxonomy_path: Option<PathBuf>,
    other_names_path: Option<PathBuf>,
    practice_locations_path: Option<PathBuf>,
    endpoints_path: Option<PathBuf>,
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
                    state_index.entry(state.as_code().to_string())
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
                    state_index.entry(state.as_code().to_string())
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
        let state_enum = StateCode::from_code(state);
        if let Some(index) = &self.state_index {
            if let Some(state_enum) = &state_enum {
                index.get(state_enum.as_code())
                    .map(|indices| {
                        indices.iter()
                            .filter_map(|&idx| self.providers.get(idx))
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        } else {
            self.providers.iter()
                .filter(|p| {
                    p.mailing_address.state.as_ref()
                        .map(|s| Some(s) == state_enum.as_ref())
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
        let state_enum = StateCode::from_code(state);
        self.filters.push(Box::new(move |p| {
            p.mailing_address.state.as_ref()
                .map(|s| Some(s) == state_enum.as_ref())
                .unwrap_or(false)
        }));
        self
    }
    
    /// Filter by multiple states
    pub fn state_in(mut self, states: &'a [&str]) -> Self {
        let state_enums: Vec<_> = states.iter().filter_map(|s| StateCode::from_code(s)).collect();
        self.filters.push(Box::new(move |p| {
            p.mailing_address.state.as_ref()
                .map(|s| state_enums.iter().any(|se| se == s))
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
        let entity_type = entity_type.clone();
        self.filters.push(Box::new(move |p| p.entity_type == Some(entity_type.clone())));
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
                Some(EntityType::Individual) => stats.individual_providers += 1,
                Some(EntityType::Organization) => stats.organization_providers += 1,
                None => {},
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