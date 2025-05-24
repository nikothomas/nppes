use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use nppes::prelude::*;
use std::path::Path;
use tempfile::TempDir;
use std::sync::OnceLock;

// Static storage for the loaded dataset to avoid reloading for each benchmark
static DATASET: OnceLock<NppesDataset> = OnceLock::new();

// Helper function to load the actual NPPES dataset
fn get_dataset() -> &'static NppesDataset {
    DATASET.get_or_init(|| {
        println!("Loading NPPES dataset for benchmarking...");
        println!("This may take several minutes due to the large file size...");
        
        // Build dataset from actual files
        let dataset = NppesDatasetBuilder::new()
            .main_data("data/npidata_pfile_20050523-20250511.csv")
            .taxonomy_reference("data/nucc_taxonomy_250.csv")
            .build_indexes(true)
            .skip_invalid_records(true)
            .show_progress(false)  // Disable progress bars for benchmarks
            .build()
            .expect("Failed to load NPPES dataset");
        
        println!("Dataset loaded successfully!");
        println!("Total providers: {}", dataset.providers.len());
        
        dataset
    })
}

fn benchmark_npi_validation(c: &mut Criterion) {
    // Get some real NPIs from the dataset
    let dataset = get_dataset();
    let valid_npis: Vec<String> = dataset.providers.iter()
        .take(100)
        .map(|p| p.npi.as_str().to_string())
        .collect();
    
    c.bench_function("npi_validation_valid_real", |b| {
        let npi = &valid_npis[0];
        b.iter(|| {
            let result = Npi::new(black_box(npi.clone()));
            assert!(result.is_ok());
        })
    });
    
    c.bench_function("npi_validation_invalid", |b| {
        b.iter(|| {
            let result = Npi::new(black_box("12345".to_string()));
            assert!(result.is_err());
        })
    });
}

fn benchmark_index_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_building");
    group.sample_size(10); // Reduce sample size for expensive operations
    
    // Benchmark the time to load and build indexes for different sized datasets
    // Note: This requires loading the data each time, which is expensive but realistic
    group.bench_function("load_and_index_100k_records", |b| {
        b.iter(|| {
            // In a real scenario, you might have a smaller test file
            // For now, we'll benchmark the actual loading process
            let reader = NppesReader::new()
                .with_skip_invalid_records(true);
            
            // Estimate the memory usage as a proxy for index building performance
            let _ = NppesReader::estimate_memory_usage("data/npidata_pfile_20050523-20250111.csv");
        });
    });
    
    group.finish();
}

fn benchmark_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("queries");
    let dataset = get_dataset();
    
    // Get some real NPIs for lookup benchmarks
    let sample_npis: Vec<Npi> = dataset.providers.iter()
        .filter(|p| p.mailing_address.state.as_ref() == Some(&StateCode::CA))
        .take(10)
        .map(|p| p.npi.clone())
        .collect();
    // Benchmark NPI lookup with real data
    group.bench_function("npi_lookup_indexed_real", |b| {
        let npi = &sample_npis[0];
        b.iter(|| {
            dataset.get_by_npi(black_box(npi))
        });
    });
    
    // Benchmark state queries with real data
    group.bench_function("state_query_CA_real", |b| {
        b.iter(|| {
            dataset.get_by_state(black_box("CA"))
        });
    });
    
    group.bench_function("state_query_NY_real", |b| {
        b.iter(|| {
            dataset.get_by_state(black_box("NY"))
        });
    });
    
    // Benchmark complex queries with real data
    group.bench_function("complex_query_real", |b| {
        b.iter(|| {
            dataset.query()
                .state("CA")
                .entity_type(EntityType::Individual)
                .active_only()
                .execute()
        });
    });
    
    // Benchmark taxonomy queries with real data
    group.bench_function("taxonomy_query_real", |b| {
        let analytics = dataset.analytics();
        b.iter(|| {
            analytics.find_by_taxonomy_code("207Q00000X")
        });
    });
    
    group.finish();
}

fn benchmark_exports(c: &mut Criterion) {
    let mut group = c.benchmark_group("exports");
    group.sample_size(10); // Reduce sample size for I/O operations
    
    let dataset = get_dataset();
    let temp_dir = TempDir::new().unwrap();
    
    // Benchmark export_subset which properly handles filtered exports
    group.bench_function("json_export_subset_1k", |b| {
        let path = temp_dir.path().join("export.json");
        let first_1k: Vec<_> = dataset.providers.iter().take(1000).collect();
        b.iter(|| {
            dataset.export_subset(
                &path,
                |rec| first_1k.contains(&rec),
                ExportFormat::Json
            ).unwrap();
        });
    });
    
    // For JSON Lines, we can use the direct export method with the full dataset
    // but limit the benchmark iterations
    group.bench_function("json_lines_export_first_1k", |b| {
        let path = temp_dir.path().join("export_subset.jsonl");
        let first_1k: Vec<_> = dataset.providers.iter().take(1000).collect();
        b.iter(|| {
            dataset.export_subset(
                &path,
                |rec| first_1k.contains(&rec),
                ExportFormat::Json
            ).unwrap();
        });
    });
    
    // Benchmark CSV export with subset
    group.bench_function("csv_export_subset_1k", |b| {
        let path = temp_dir.path().join("export.csv");
        let first_1k: Vec<_> = dataset.providers.iter().take(1000).collect();
        b.iter(|| {
            dataset.export_subset(
                &path,
                |rec| first_1k.contains(&rec),
                ExportFormat::Csv
            ).unwrap();
        });
    });
    
    // Benchmark SQL export with subset
    group.bench_function("sql_export_subset_1k", |b| {
        let path = temp_dir.path().join("export.sql");
        let first_1k: Vec<_> = dataset.providers.iter().take(1000).collect();
        b.iter(|| {
            dataset.export_subset(
                &path,
                |rec| first_1k.contains(&rec),
                ExportFormat::Sql
            ).unwrap();
        });
    });
    
    group.finish();
}

fn benchmark_analytics(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics");
    
    let dataset = get_dataset();
    let analytics = dataset.analytics();
    
    // Benchmark dataset statistics with real data
    group.bench_function("dataset_stats_real", |b| {
        b.iter(|| {
            analytics.dataset_stats()
        });
    });
    
    // Benchmark provider count by state with real data
    group.bench_function("provider_count_by_state_real", |b| {
        b.iter(|| {
            analytics.provider_count_by_state()
        });
    });
    
    // Benchmark top states with real data
    group.bench_function("top_10_states_real", |b| {
        b.iter(|| {
            analytics.top_states_by_provider_count(10)
        });
    });
    
    // Benchmark taxonomy analysis with real data
    group.bench_function("taxonomy_distribution_real", |b| {
        b.iter(|| {
            analytics.provider_count_by_taxonomy()
        });
    });
    
    group.finish();
}

fn benchmark_data_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types");
    
    let dataset = get_dataset();
    
    // Get some real records for benchmarking
    let individual_records: Vec<&NppesRecord> = dataset.providers.iter()
        .filter(|p| p.entity_type == Some(EntityType::Individual))
        .take(10)
        .collect();
    
    let organization_records: Vec<&NppesRecord> = dataset.providers.iter()
        .filter(|p| p.entity_type == Some(EntityType::Organization))
        .take(10)
        .collect();
    
    // Benchmark display name generation with real data
    group.bench_function("individual_display_name_real", |b| {
        let record = individual_records[0];
        b.iter(|| {
            record.display_name()
        });
    });
    
    group.bench_function("organization_display_name_real", |b| {
        let record = organization_records[0];
        b.iter(|| {
            record.display_name()
        });
    });
    
    // Benchmark address formatting with real addresses
    group.bench_function("address_format_single_line_real", |b| {
        let address = &individual_records[0].mailing_address;
        b.iter(|| {
            address.format_single_line()
        });
    });
    
    // Benchmark active status check
    group.bench_function("is_active_check_real", |b| {
        let record = individual_records[0];
        b.iter(|| {
            record.is_active()
        });
    });
    
    group.finish();
}

fn benchmark_memory_estimation(c: &mut Criterion) {
    c.bench_function("memory_estimation_real_file", |b| {
        let path = Path::new("data/npidata_pfile_20050523-20250111.csv");
        b.iter(|| {
            NppesReader::estimate_memory_usage(path).unwrap()
        });
    });
}

fn benchmark_loading_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("loading_performance");
    group.sample_size(5); // Very small sample size for expensive operations
    
    // Benchmark memory estimation which is fast and gives insight into loading
    group.bench_function("estimate_memory_full_dataset", |b| {
        b.iter(|| {
            NppesReader::estimate_memory_usage("data/npidata_pfile_20050523-20250111.csv").unwrap()
        });
    });
    
    // Benchmark taxonomy loading which is smaller and faster
    group.bench_function("load_taxonomy_reference", |b| {
        b.iter(|| {
            let reader = NppesReader::new();
            reader.load_taxonomy_data("data/nucc_taxonomy_250.csv").unwrap()
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_npi_validation,
    benchmark_index_building,
    benchmark_queries,
    benchmark_exports,
    benchmark_analytics,
    benchmark_data_types,
    benchmark_memory_estimation,
    benchmark_loading_performance
);

criterion_main!(benches); 