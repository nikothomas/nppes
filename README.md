# NPPES Data Library

A comprehensive Rust library for working with National Plan and Provider Enumeration System (NPPES) healthcare provider data.

## Overview

The NPPES dataset contains information about healthcare providers in the United States, including:
- ~8 million healthcare provider records
- 330+ data columns including NPI numbers, provider information, taxonomy codes
- Entity types: Individual providers (code 1) vs Organizations (code 2) 
- Healthcare provider taxonomy codes for specialties
- Geographic information and licensing data

## Features

- **Type-safe data structures** for all NPPES file formats
- **CSV parsing and loading** with validation and error handling  
- **Analytics and querying** functionality for data analysis
- **Schema validation** against official NPPES documentation
- **Support for all NPPES reference files** (Other Names, Practice Locations, Endpoints)

## NPPES Data Files Supported

### Main Data File
- File: `npidata_pfile_yyyymmdd-yyyymmdd.csv` (9.9GB)
- Contains: ~8M healthcare provider records with 330+ columns

### Reference Files
- **Other Name Reference**: Additional organization names for Type 2 NPIs
- **Practice Location Reference**: Non-primary practice locations
- **Endpoint Reference**: Healthcare endpoints associated with NPIs  
- **Taxonomy Reference**: Healthcare provider classification codes (NUCC)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nppes = "0.0.3
```

The CLI binary is called `npcli`.

## Usage

### Basic Usage

```rust
use nppes::prelude::*;

// Load main NPPES data
let reader = NppesReader::new();
let providers = reader.load_main_data("data/npidata_pfile_20050523-20250511.csv")?;

println!("Loaded {} providers", providers.len());

// Load taxonomy reference data
let taxonomy_data = reader.load_taxonomy_data("data/nucc_taxonomy_250.csv")?;
```

### Command Line Interface (CLI)

You can use the CLI tool `npcli` to download, query, and export NPPES data.

#### Example: Download the latest NPPES data

```sh
npcli download --out-dir ./data
```

#### Example: Show statistics for a dataset

```sh
npcli stats --data-dir ./data
```

#### Example: Query providers by state and specialty

```sh
npcli query --data-dir ./data --state CA --specialty Cardiology
```

#### Example: Export data to JSON

```sh
npcli export --data-dir ./data --output ca_cardiologists.json --format json --state CA --specialty Cardiology
```

### Analytics and Querying

```rust
use nppes::prelude::*;

// Create analytics engine
let analytics = NppesAnalytics::new(&providers)
    .with_taxonomy_reference(&taxonomy_data);

// Get dataset statistics
let stats = analytics.dataset_stats();
stats.print_summary();

// Find providers by state
let ca_providers = analytics.find_by_state("CA");
println!("California providers: {}", ca_providers.len());

// Find providers by taxonomy code
let physicians = analytics.find_by_taxonomy_code("208600000X");
println!("Internal Medicine physicians: {}", physicians.len());

// Complex queries with builder pattern
let query_results = ProviderQuery::new(&analytics)
    .entity_type(EntityType::Individual)
    .state("NY")
    .active_only()
    .execute();

println!("Active individual providers in NY: {}", query_results.len());
```

### Working with Individual Records

```rust
use nppes::prelude::*;

// Find a specific provider by NPI
let npi = Npi::new("1234567890".to_string())?;
if let Some(provider) = analytics.find_by_npi(&npi) {
    println!("Provider: {}", provider.display_name());
    println!("Entity Type: {:?}", provider.entity_type);
    println!("Active: {}", provider.is_active());
    
    // Get primary taxonomy
    if let Some(primary_taxonomy) = provider.primary_taxonomy() {
        println!("Primary specialty: {}", primary_taxonomy.code);
    }
}
```

### Data Enrichment

```rust
use nppes::prelude::*;

// Enrich providers with human-readable taxonomy descriptions
let enriched_providers = analytics.enrich_with_taxonomy_descriptions()?;

for enriched in enriched_providers.iter().take(10) {
    println!("Provider: {}", enriched.provider.display_name());
    
    for taxonomy in &enriched.enriched_taxonomies {
        if let Some(display_name) = &taxonomy.display_name {
            println!("  Specialty: {}", display_name);
        }
    }
}
```

### Advanced Analytics

```rust
use nppes::prelude::*;

// Get top states by provider count
let top_states = analytics.top_states_by_provider_count(10);
for (state, count) in top_states {
    println!("{}: {} providers", state, count);
}

// Get top specialties
let top_specialties = analytics.top_taxonomy_codes_by_provider_count(10);
for (code, count) in top_specialties {
    if let Some(taxonomy_ref) = analytics.get_taxonomy_description(&code) {
        if let Some(display_name) = &taxonomy_ref.display_name {
            println!("{}: {} providers", display_name, count);
        }
    }
}

// Date-based queries
use chrono::NaiveDate;
let start_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
let end_date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

let new_providers = analytics.providers_enumerated_between(start_date, end_date);
println!("Providers enumerated in 2023: {}", new_providers.len());
```

## Configuration Options

### Reader Configuration

```rust
use nppes::prelude::*;

let reader = NppesReader::new()
    .with_header_validation(true)  // Validate CSV headers (default: true)
    .with_skip_invalid_records(false); // Skip invalid records (default: false)
```

### Error Handling

The library uses a comprehensive error system:

```rust
use nppes::prelude::*;

match reader.load_main_data("invalid_path.csv") {
    Ok(providers) => println!("Loaded {} providers", providers.len()),
    Err(NppesError::FileNotFound(path)) => {
        eprintln!("File not found: {}", path);
    }
    Err(NppesError::CsvParse(msg)) => {
        eprintln!("CSV parsing error: {}", msg);
    }
    Err(NppesError::DataValidation(msg)) => {
        eprintln!("Data validation error: {}", msg);
    }
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Data Structures

### Core Types

- `NppesRecord`: Main provider record with all NPPES data
- `EntityType`: Individual vs Organization provider type
- `Npi`: Type-safe NPI number wrapper
- `TaxonomyCode`: Healthcare specialty/taxonomy information
- `Address`: Mailing and practice location addresses

### Reference Types

- `TaxonomyReference`: Healthcare taxonomy lookup data
- `OtherNameRecord`: Additional organization names
- `PracticeLocationRecord`: Secondary practice locations  
- `EndpointRecord`: Healthcare endpoints

## Performance Considerations

- The main NPPES file is 9.9GB with ~8M records
- Recommend 16GB+ RAM for full dataset processing
- Use streaming or chunked processing for memory-constrained environments
- Consider creating indexes for frequently queried fields

## License

MIT License

## Contributing

Contributions welcome! Please see CONTRIBUTING.md for guidelines. 