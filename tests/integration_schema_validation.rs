/*!
 * Integration test for NPPES schema validation
 * 
 * This test downloads the latest NPPES data from CMS and validates that our
 * schema definitions match the actual data format. This ensures our library
 * works correctly with real NPPES data files.
 * 
 * Note: This test requires network access and may take several minutes to run
 * due to the large size of NPPES data files (several GB).
 */

#[cfg(all(test, feature = "download"))]
mod schema_validation_tests {
    use nppes::prelude::*;
    use nppes::download::{NppesDownloader, DownloadConfig};
    use nppes::schema::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use std::fs::File;
    use csv::{ReaderBuilder, StringRecord};

    /// Test configuration for integration tests
    struct TestConfig {
        /// Only test the first N records to speed up tests
        max_test_records: usize,
        /// Whether to keep downloaded files for inspection
        keep_files: bool,
        /// Temporary directory for downloads
        temp_dir: TempDir,
    }

    impl TestConfig {
        fn new() -> std::io::Result<Self> {
            Ok(Self {
                max_test_records: 1000, // Test first 1000 records by default
                keep_files: std::env::var("NPPES_KEEP_TEST_FILES").is_ok(),
                temp_dir: TempDir::new()?,
            })
        }
    }

    /// Download and validate NPPES main data file schema
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_main_data_schema_validation() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        // Configure downloader to use our temp directory
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600, // 10 minutes for large files
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        println!("Extracted files: {}", extracted.summary());
        
        // Test main data file
        if let Some(main_data_file) = &extracted.main_data_file {
            println!("Testing main data file schema: {}", main_data_file.display());
            test_csv_schema_validation(
                main_data_file,
                &NppesMainSchema::column_names(),
                "Main NPPES Data",
                test_config.max_test_records
            ).expect("Main data schema validation failed");
        } else {
            panic!("Main data file not found in extracted files");
        }
        
        // Test if we can actually parse some records
        if let Some(main_data_file) = &extracted.main_data_file {
            test_record_parsing(main_data_file, test_config.max_test_records)
                .expect("Failed to parse main data records");
        }
    }

    /// Test practice location data schema validation
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_practice_location_schema_validation() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for practice location testing...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        // Test practice location file if it exists
        if let Some(practice_locations_file) = &extracted.practice_locations_file {
            println!("Testing practice locations schema: {}", practice_locations_file.display());
            test_csv_schema_validation(
                practice_locations_file,
                &PracticeLocationSchema::column_names(),
                "Practice Locations",
                test_config.max_test_records
            ).expect("Practice locations schema validation failed");
            
            // Test parsing practice location records
            test_practice_location_parsing(practice_locations_file, test_config.max_test_records)
                .expect("Failed to parse practice location records");
        } else {
            println!("Practice locations file not found - this is normal for some NPPES releases");
        }
    }

    /// Test other names data schema validation
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_other_names_schema_validation() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for other names testing...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        // Test other names file if it exists
        if let Some(other_names_file) = &extracted.other_names_file {
            println!("Testing other names schema: {}", other_names_file.display());
            test_csv_schema_validation(
                other_names_file,
                &OtherNameSchema::column_names(),
                "Other Names",
                test_config.max_test_records
            ).expect("Other names schema validation failed");
            
            // Test parsing other name records
            test_other_name_parsing(other_names_file, test_config.max_test_records)
                .expect("Failed to parse other name records");
        } else {
            println!("Other names file not found - this is normal for some NPPES releases");
        }
    }

    /// Test endpoints data schema validation
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_endpoints_schema_validation() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for endpoints testing...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        // Test endpoints file if it exists
        if let Some(endpoints_file) = &extracted.endpoints_file {
            println!("Testing endpoints schema: {}", endpoints_file.display());
            test_csv_schema_validation(
                endpoints_file,
                &EndpointSchema::column_names(),
                "Endpoints",
                test_config.max_test_records
            ).expect("Endpoints schema validation failed");
            
            // Test parsing endpoint records
            test_endpoint_parsing(endpoints_file, test_config.max_test_records)
                .expect("Failed to parse endpoint records");
        } else {
            println!("Endpoints file not found - this is normal for some NPPES releases");
        }
    }

    /// Test taxonomy reference data schema validation
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_taxonomy_schema_validation() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for taxonomy testing...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        // Test taxonomy file if it exists
        if let Some(taxonomy_file) = &extracted.taxonomy_file {
            println!("Testing taxonomy schema: {}", taxonomy_file.display());
            test_csv_schema_validation(
                taxonomy_file,
                &TaxonomySchema::column_names(),
                "Taxonomy Reference",
                test_config.max_test_records
            ).expect("Taxonomy schema validation failed");
            
            // Test parsing taxonomy records
            test_taxonomy_parsing(taxonomy_file, test_config.max_test_records)
                .expect("Failed to parse taxonomy records");
        } else {
            println!("Taxonomy file not found - this is normal for some NPPES releases");
        }
    }

    /// Test complete dataset loading with downloaded data
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_complete_dataset_loading() {
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for complete dataset test...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        // Try to load a small subset of the data
        if let Some(main_data_file) = &extracted.main_data_file {
            println!("Testing complete dataset loading with real NPPES data...");
            
            // Create a subset file with just the first few records for testing
            let subset_file = create_test_subset(main_data_file, 100)
                .expect("Failed to create test subset");
            
            // Try to load the subset using our library
            let reader = NppesReader::new();
            let records = reader.load_main_data(&subset_file)
                .expect("Should be able to load NPPES records");
            
            assert!(!records.is_empty(), "Should have loaded some records");
            println!("Successfully loaded {} records from real NPPES data", records.len());
            
            // Validate some basic properties
            for record in &records[..std::cmp::min(10, records.len())] {
                assert!(!record.npi.as_str().is_empty(), "NPI should not be empty");
                // Note: entity_type might be None for some real NPPES records
                // This is acceptable as the field can be empty or contain unexpected values
                
                // Test display methods
                let _ = record.display_name();
                let _ = record.full_display_name();
                let _ = record.is_active();
            }
            
            // Clean up subset file
            let _ = std::fs::remove_file(&subset_file);
        }
    }

    /// Helper function to validate CSV schema against expected columns
    fn test_csv_schema_validation(
        file_path: &PathBuf,
        expected_columns: &[&str],
        file_type: &str,
        max_records: usize,
    ) -> nppes::Result<()> {
        println!("Validating {} schema with {} expected columns", file_type, expected_columns.len());
        
        let file = File::open(file_path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        // Get headers
        let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
        
        println!("Found {} columns in {}", headers.len(), file_type);
        
        // Check column count
        if headers.len() != expected_columns.len() {
            println!("Column count mismatch!");
            println!("Expected: {}", expected_columns.len());
            println!("Found: {}", headers.len());
            
            // Show first few actual columns for debugging
            println!("First 10 actual columns:");
            for (i, header) in headers.iter().take(10).enumerate() {
                println!("  {}: {}", i, header);
            }
            
            return Err(NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        // Check each column name
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                println!("Column {} mismatch:", i);
                println!("  Expected: '{}'", expected);
                println!("  Found: '{}'", actual);
                
                return Err(NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        // Test parsing a few records to ensure the schema works in practice
        let mut record_count = 0;
        for result in reader.records() {
            if record_count >= max_records {
                break;
            }
            
            let record = result?;
            assert_eq!(record.len(), expected_columns.len(), 
                "Record {} has wrong number of fields", record_count);
            
            record_count += 1;
        }
        
        println!("✓ {} schema validation passed! Tested {} records", file_type, record_count);
        Ok(())
    }

    /// Test parsing actual main data records
    fn test_record_parsing(file_path: &PathBuf, max_records: usize) -> nppes::Result<()> {
        println!("Testing main data record parsing...");
        
        let reader = NppesReader::new();
        
        // Create a subset file for testing
        let subset_file = create_test_subset(file_path, max_records)?;
        
        // Try to parse records
        let records = reader.load_main_data(&subset_file)?;
        
        assert!(!records.is_empty(), "Should have parsed some records");
        println!("✓ Successfully parsed {} main data records", records.len());
        
        // Clean up
        let _ = std::fs::remove_file(&subset_file);
        
        Ok(())
    }

    /// Test parsing practice location records
    fn test_practice_location_parsing(file_path: &PathBuf, max_records: usize) -> nppes::Result<()> {
        println!("Testing practice location record parsing...");
        
        let reader = NppesReader::new();
        
        // Create a subset file for testing
        let subset_file = create_test_subset(file_path, max_records)?;
        
        // Try to parse records
        let records = reader.load_practice_location_data(&subset_file)?;
        
        println!("✓ Successfully parsed {} practice location records", records.len());
        
        // Clean up
        let _ = std::fs::remove_file(&subset_file);
        
        Ok(())
    }

    /// Test parsing other name records
    fn test_other_name_parsing(file_path: &PathBuf, max_records: usize) -> nppes::Result<()> {
        println!("Testing other name record parsing...");
        
        let reader = NppesReader::new();
        
        // Create a subset file for testing
        let subset_file = create_test_subset(file_path, max_records)?;
        
        // Try to parse records
        let records = reader.load_other_name_data(&subset_file)?;
        
        println!("✓ Successfully parsed {} other name records", records.len());
        
        // Clean up
        let _ = std::fs::remove_file(&subset_file);
        
        Ok(())
    }

    /// Test parsing endpoint records
    fn test_endpoint_parsing(file_path: &PathBuf, max_records: usize) -> nppes::Result<()> {
        println!("Testing endpoint record parsing...");
        
        let reader = NppesReader::new();
        
        // Create a subset file for testing
        let subset_file = create_test_subset(file_path, max_records)?;
        
        // Try to parse records
        let records = reader.load_endpoint_data(&subset_file)?;
        
        println!("✓ Successfully parsed {} endpoint records", records.len());
        
        // Clean up
        let _ = std::fs::remove_file(&subset_file);
        
        Ok(())
    }

    /// Test parsing taxonomy records
    fn test_taxonomy_parsing(file_path: &PathBuf, max_records: usize) -> nppes::Result<()> {
        println!("Testing taxonomy record parsing...");
        
        let reader = NppesReader::new();
        
        // Create a subset file for testing
        let subset_file = create_test_subset(file_path, max_records)?;
        
        // Try to parse records
        let records = reader.load_taxonomy_data(&subset_file)?;
        
        println!("✓ Successfully parsed {} taxonomy records", records.len());
        
        // Clean up
        let _ = std::fs::remove_file(&subset_file);
        
        Ok(())
    }

    /// Create a subset file with the header and first N data rows
    fn create_test_subset(original_file: &PathBuf, max_records: usize) -> nppes::Result<PathBuf> {
        let file = File::open(original_file)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        
        // Create temp file for subset
        let temp_file = original_file.with_extension("subset.csv");
        let mut writer = csv::WriterBuilder::new()
            .has_headers(true)
            .from_path(&temp_file)?;
        
        // Write headers
        writer.write_record(reader.headers()?)?;
        
        // Write first N records
        let mut count = 0;
        for result in reader.records() {
            if count >= max_records {
                break;
            }
            
            let record = result?;
            writer.write_record(&record)?;
            count += 1;
        }
        
        writer.flush()?;
        
        Ok(temp_file)
    }

    /// Performance test to make sure schema validation is reasonably fast
    #[tokio::test]
     // Run with: cargo test --features download
    async fn test_schema_validation_performance() {
        use std::time::Instant;
        
        let test_config = TestConfig::new().expect("Failed to create test config");
        
        let download_config = DownloadConfig {
            download_dir: Some(test_config.temp_dir.path().to_path_buf()),
            keep_files: test_config.keep_files,
            timeout_seconds: 600,
            ..Default::default()
        };
        
        let mut downloader = NppesDownloader::with_config(download_config);
        
        println!("Downloading latest NPPES data for performance testing...");
        let extracted = downloader.download_latest_nppes().await
            .expect("Should be able to download NPPES data");
        
        if let Some(main_data_file) = &extracted.main_data_file {
            let start = Instant::now();
            
            // Test header validation performance
            let file = File::open(main_data_file).expect("Should open file");
            let mut reader = ReaderBuilder::new()
                .has_headers(true)
                .from_reader(file);
            
            let headers: Vec<String> = reader.headers()
                .expect("Should read headers")
                .iter()
                .map(|s| s.to_string())
                .collect();
            
            let validation_result = NppesMainSchema::validate_headers(&headers);
            let elapsed = start.elapsed();
            
            println!("Schema validation took: {:?}", elapsed);
            assert!(elapsed.as_millis() < 1000, "Schema validation should be fast (< 1 second)");
            
            if let Err(e) = validation_result {
                println!("Schema validation error: {}", e);
                panic!("Schema validation failed");
            }
        }
    }
}

// Tests that run without the download feature
#[cfg(test)]
mod unit_tests {
    use nppes::schema::*;
    
    #[test]
    fn test_schema_column_counts() {
        // Ensure our schemas have the expected number of columns
        assert!(NppesMainSchema::column_count() > 300, "Main schema should have 330+ columns");
        assert_eq!(OtherNameSchema::column_count(), 3, "Other name schema should have 3 columns");
        assert_eq!(PracticeLocationSchema::column_count(), 10, "Practice location schema should have 10 columns");
        assert_eq!(EndpointSchema::column_count(), 19, "Endpoint schema should have 19 columns");
        assert_eq!(TaxonomySchema::column_count(), 8, "Taxonomy schema should have 8 columns");
    }
    
    #[test]
    fn test_practice_location_schema_specific_columns() {
        let columns = PracticeLocationSchema::column_names();
        
        // Test the specific column that was causing issues
        assert_eq!(columns[2], "Provider Secondary Practice Location Address-  Address Line 2", 
            "Column 2 should have double space after dash");
        assert_eq!(columns[1], "Provider Secondary Practice Location Address- Address Line 1",
            "Column 1 should have single space after dash");
    }
    
    #[test]
    fn test_main_schema_includes_expected_columns() {
        let columns = NppesMainSchema::column_names();
        
        // Test some key columns exist
        assert!(columns.contains(&"NPI"), "Should contain NPI column");
        assert!(columns.contains(&"Entity Type Code"), "Should contain Entity Type Code");
        assert!(columns.contains(&"Provider Organization Name (Legal Business Name)"), 
            "Should contain organization name column");
        assert!(columns.contains(&"Provider Last Name (Legal Name)"), 
            "Should contain individual last name column");
    }
}