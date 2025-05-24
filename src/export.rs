/*!
 * Export functionality for NPPES data
 * 
 * Provides various export formats for NPPES data including JSON, CSV,
 * SQL, and optionally Parquet and Arrow formats.
 */

use std::path::Path;
use std::fs::File;
use std::io::{Write, BufWriter};
use serde_json;

use crate::{Result, NppesError, ExportFormat};
use crate::data_types::*;
use crate::dataset::NppesDataset;

#[cfg(feature = "arrow-export")]
use arrow::array::*;
#[cfg(feature = "arrow-export")]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "arrow-export")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "arrow-export")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "arrow-export")]
use parquet::arrow::arrow_reader::ParquetFileArrowReader;
#[cfg(feature = "arrow-export")]
use std::sync::Arc;

/// Trait for implementing NPPES data exporters
pub trait NppesExporter {
    /// Export the dataset
    fn export(&self, dataset: &NppesDataset, path: &Path) -> Result<()>;
    
    /// Get the export format
    fn format(&self) -> ExportFormat;
}

/// JSON exporter for NPPES data
pub struct JsonExporter {
    /// Whether to pretty-print the JSON
    pub pretty_print: bool,
    /// Whether to include empty/null fields
    pub include_empty_fields: bool,
    /// Whether to export as JSON Lines (one record per line)
    pub json_lines: bool,
}

impl Default for JsonExporter {
    fn default() -> Self {
        Self {
            pretty_print: true,
            include_empty_fields: false,
            json_lines: false,
        }
    }
}

impl JsonExporter {
    /// Create a new JSON exporter
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set pretty printing
    pub fn with_pretty_print(mut self, pretty: bool) -> Self {
        self.pretty_print = pretty;
        self
    }
    
    /// Set whether to include empty fields
    pub fn with_empty_fields(mut self, include: bool) -> Self {
        self.include_empty_fields = include;
        self
    }
    
    /// Set JSON Lines format
    pub fn as_json_lines(mut self) -> Self {
        self.json_lines = true;
        self.pretty_print = false; // JSON Lines shouldn't be pretty printed
        self
    }
}

impl NppesExporter for JsonExporter {
    fn export(&self, dataset: &NppesDataset, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        
        if self.json_lines {
            // Export as JSON Lines (one record per line)
            for provider in &dataset.providers {
                let json = serde_json::to_string(&provider)?;
                writeln!(writer, "{}", json)?;
            }
        } else {
            // Export as single JSON array
            if self.pretty_print {
                serde_json::to_writer_pretty(writer, &dataset.providers)?;
            } else {
                serde_json::to_writer(writer, &dataset.providers)?;
            }
        }
        
        Ok(())
    }
    
    fn format(&self) -> ExportFormat {
        ExportFormat::Json
    }
}

/// CSV exporter for NPPES data
/// 
/// Exports data in a normalized format with separate files for related data
pub struct CsvExporter {
    /// Whether to include headers
    pub include_headers: bool,
    /// Field delimiter
    pub delimiter: u8,
    /// Whether to normalize into multiple files
    pub normalize: bool,
}

impl Default for CsvExporter {
    fn default() -> Self {
        Self {
            include_headers: true,
            delimiter: b',',
            normalize: true,
        }
    }
}

impl CsvExporter {
    /// Create a new CSV exporter
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the delimiter
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }
    
    /// Set normalization
    pub fn with_normalization(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }
}

impl NppesExporter for CsvExporter {
    fn export(&self, dataset: &NppesDataset, path: &Path) -> Result<()> {
        if self.normalize {
            self.export_normalized(dataset, path)
        } else {
            self.export_denormalized(dataset, path)
        }
    }
    
    fn format(&self) -> ExportFormat {
        ExportFormat::Csv
    }
}

impl CsvExporter {
    fn export_normalized(&self, dataset: &NppesDataset, base_path: &Path) -> Result<()> {
        // Create directory for normalized files
        let dir = base_path.parent().unwrap_or(Path::new("."));
        let base_name = base_path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("nppes_export");
        
        // Export main provider data
        let providers_path = dir.join(format!("{}_providers.csv", base_name));
        let providers_file = File::create(&providers_path)?;
        let mut providers_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.include_headers)
            .from_writer(providers_file);
        
        // Write provider records (simplified - would need custom serialization for full data)
        for provider in &dataset.providers {
            providers_writer.write_record(&[
                provider.npi.as_str(),
                provider.entity_type.to_code(),
                &provider.display_name(),
                provider.mailing_address.state.as_deref().unwrap_or(""),
                provider.mailing_address.postal_code.as_deref().unwrap_or(""),
            ])?;
        }
        providers_writer.flush()?;
        
        // Export taxonomy codes
        let taxonomy_path = dir.join(format!("{}_taxonomies.csv", base_name));
        let taxonomy_file = File::create(&taxonomy_path)?;
        let mut taxonomy_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.include_headers)
            .from_writer(taxonomy_file);
        
        if self.include_headers {
            taxonomy_writer.write_record(&["npi", "taxonomy_code", "is_primary", "license_number", "license_state"])?;
        }
        
        for provider in &dataset.providers {
            for taxonomy in &provider.taxonomy_codes {
                taxonomy_writer.write_record(&[
                    provider.npi.as_str(),
                    &taxonomy.code,
                    if taxonomy.is_primary { "Y" } else { "N" },
                    taxonomy.license_number.as_deref().unwrap_or(""),
                    taxonomy.license_state.as_deref().unwrap_or(""),
                ])?;
            }
        }
        taxonomy_writer.flush()?;
        
        println!("Exported normalized CSV files to: {}", dir.display());
        Ok(())
    }
    
    fn export_denormalized(&self, dataset: &NppesDataset, path: &Path) -> Result<()> {
        // Export as single denormalized file (similar to original NPPES format)
        Err(NppesError::Custom {
            message: "Denormalized CSV export not yet implemented".to_string(),
            suggestion: Some("Use normalized export or JSON export instead".to_string()),
        })
    }
}

/// SQL exporter for NPPES data
pub struct SqlExporter {
    /// SQL dialect to use
    pub dialect: SqlDialect,
    /// Table name prefix
    pub table_prefix: String,
    /// Batch size for insert statements
    pub batch_size: usize,
    /// Whether to include CREATE TABLE statements
    pub include_schema: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum SqlDialect {
    PostgreSQL,
    MySQL,
    SQLite,
    SqlServer,
}

impl Default for SqlExporter {
    fn default() -> Self {
        Self {
            dialect: SqlDialect::PostgreSQL,
            table_prefix: "nppes".to_string(),
            batch_size: 1000,
            include_schema: true,
        }
    }
}

impl SqlExporter {
    /// Create a new SQL exporter
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the SQL dialect
    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }
    
    /// Set the table prefix
    pub fn with_table_prefix(mut self, prefix: String) -> Self {
        self.table_prefix = prefix;
        self
    }
}

impl NppesExporter for SqlExporter {
    fn export(&self, dataset: &NppesDataset, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        
        if self.include_schema {
            self.write_schema(&mut writer)?;
        }
        
        // Write provider inserts
        writeln!(writer, "\n-- Provider data")?;
        self.write_provider_inserts(&mut writer, &dataset.providers)?;
        
        Ok(())
    }
    
    fn format(&self) -> ExportFormat {
        ExportFormat::Sql
    }
}

impl SqlExporter {
    fn write_schema(&self, writer: &mut dyn Write) -> Result<()> {
        match self.dialect {
            SqlDialect::PostgreSQL => {
                writeln!(writer, "-- NPPES Database Schema for PostgreSQL\n")?;
                writeln!(writer, "CREATE TABLE IF NOT EXISTS {}_providers (", self.table_prefix)?;
                writeln!(writer, "  npi VARCHAR(10) PRIMARY KEY,")?;
                writeln!(writer, "  entity_type SMALLINT NOT NULL,")?;
                writeln!(writer, "  organization_name VARCHAR(255),")?;
                writeln!(writer, "  last_name VARCHAR(100),")?;
                writeln!(writer, "  first_name VARCHAR(100),")?;
                writeln!(writer, "  middle_name VARCHAR(100),")?;
                writeln!(writer, "  mailing_address_line1 VARCHAR(255),")?;
                writeln!(writer, "  mailing_address_city VARCHAR(100),")?;
                writeln!(writer, "  mailing_address_state VARCHAR(2),")?;
                writeln!(writer, "  mailing_address_postal_code VARCHAR(10),")?;
                writeln!(writer, "  enumeration_date DATE,")?;
                writeln!(writer, "  last_update_date DATE,")?;
                writeln!(writer, "  is_active BOOLEAN DEFAULT TRUE")?;
                writeln!(writer, ");\n")?;
                
                writeln!(writer, "CREATE TABLE IF NOT EXISTS {}_taxonomies (", self.table_prefix)?;
                writeln!(writer, "  id SERIAL PRIMARY KEY,")?;
                writeln!(writer, "  npi VARCHAR(10) REFERENCES {}_providers(npi),", self.table_prefix)?;
                writeln!(writer, "  taxonomy_code VARCHAR(10) NOT NULL,")?;
                writeln!(writer, "  is_primary BOOLEAN DEFAULT FALSE,")?;
                writeln!(writer, "  license_number VARCHAR(50),")?;
                writeln!(writer, "  license_state VARCHAR(2)")?;
                writeln!(writer, ");\n")?;
                
                writeln!(writer, "CREATE INDEX idx_{}_state ON {}_providers(mailing_address_state);", 
                    self.table_prefix, self.table_prefix)?;
                writeln!(writer, "CREATE INDEX idx_{}_taxonomy ON {}_taxonomies(taxonomy_code);", 
                    self.table_prefix, self.table_prefix)?;
            }
            _ => {
                writeln!(writer, "-- Schema generation for {:?} not yet implemented", self.dialect)?;
            }
        }
        Ok(())
    }
    
    fn write_provider_inserts(&self, writer: &mut dyn Write, providers: &[NppesRecord]) -> Result<()> {
        let mut count = 0;
        
        for chunk in providers.chunks(self.batch_size) {
            writeln!(writer, "INSERT INTO {}_providers (npi, entity_type, organization_name, last_name, first_name, middle_name, mailing_address_line1, mailing_address_city, mailing_address_state, mailing_address_postal_code, enumeration_date, last_update_date, is_active) VALUES", 
                self.table_prefix)?;
            
            for (i, provider) in chunk.iter().enumerate() {
                let values = match provider.entity_type {
                    EntityType::Organization => {
                        format!("('{}', {}, {}, NULL, NULL, NULL, {}, {}, {}, {}, {}, {}, {})",
                            provider.npi.as_str(),
                            provider.entity_type.to_code(),
                            sql_string(&provider.organization_name.legal_business_name),
                            sql_string(&provider.mailing_address.line_1),
                            sql_string(&provider.mailing_address.city),
                            sql_string(&provider.mailing_address.state),
                            sql_string(&provider.mailing_address.postal_code),
                            sql_date(&provider.enumeration_date),
                            sql_date(&provider.last_update_date),
                            provider.is_active()
                        )
                    }
                    EntityType::Individual => {
                        format!("('{}', {}, NULL, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            provider.npi.as_str(),
                            provider.entity_type.to_code(),
                            sql_string(&provider.provider_name.last),
                            sql_string(&provider.provider_name.first),
                            sql_string(&provider.provider_name.middle),
                            sql_string(&provider.mailing_address.line_1),
                            sql_string(&provider.mailing_address.city),
                            sql_string(&provider.mailing_address.state),
                            sql_string(&provider.mailing_address.postal_code),
                            sql_date(&provider.enumeration_date),
                            sql_date(&provider.last_update_date),
                            provider.is_active()
                        )
                    }
                };
                
                if i < chunk.len() - 1 {
                    writeln!(writer, "  {},", values)?;
                } else {
                    writeln!(writer, "  {};", values)?;
                }
            }
            
            count += chunk.len();
            if count % 10000 == 0 {
                writeln!(writer, "-- Processed {} records", count)?;
            }
        }
        
        Ok(())
    }
}

// SQL helper functions
fn sql_string(opt: &Option<String>) -> String {
    match opt {
        Some(s) => format!("'{}'", s.replace('\'', "''")),
        None => "NULL".to_string(),
    }
}

fn sql_date(opt: &Option<chrono::NaiveDate>) -> String {
    match opt {
        Some(date) => format!("'{}'", date.format("%Y-%m-%d")),
        None => "NULL".to_string(),
    }
}

/// Parquet exporter (requires "parquet" feature)
#[cfg(feature = "arrow-export")]
pub struct ParquetExporter {
    /// Compression type
    pub compression: parquet::basic::Compression,
    /// Row group size
    pub row_group_size: usize,
}

#[cfg(feature = "arrow-export")]
impl Default for ParquetExporter {
    fn default() -> Self {
        Self {
            compression: parquet::basic::Compression::SNAPPY,
            row_group_size: 100_000,
        }
    }
}

#[cfg(feature = "arrow-export")]
impl ParquetExporter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "arrow-export")]
impl NppesExporter for ParquetExporter {
    fn export(&self, dataset: &NppesDataset, path: &Path) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        // 1. Build Arrow schema (flattened, all fields)
        let schema = Arc::new(Schema::new(vec![
            Field::new("npi", DataType::Utf8, false),
            Field::new("entity_type", DataType::Utf8, false),
            Field::new("replacement_npi", DataType::Utf8, true),
            Field::new("ein", DataType::Utf8, true),
            // ProviderName
            Field::new("provider_name_prefix", DataType::Utf8, true),
            Field::new("provider_name_first", DataType::Utf8, true),
            Field::new("provider_name_middle", DataType::Utf8, true),
            Field::new("provider_name_last", DataType::Utf8, true),
            Field::new("provider_name_suffix", DataType::Utf8, true),
            Field::new("provider_name_credential", DataType::Utf8, true),
            // ProviderOtherName
            Field::new("provider_other_name_prefix", DataType::Utf8, true),
            Field::new("provider_other_name_first", DataType::Utf8, true),
            Field::new("provider_other_name_middle", DataType::Utf8, true),
            Field::new("provider_other_name_last", DataType::Utf8, true),
            Field::new("provider_other_name_suffix", DataType::Utf8, true),
            Field::new("provider_other_name_credential", DataType::Utf8, true),
            Field::new("provider_other_name_type_code", DataType::Utf8, true),
            // OrganizationName
            Field::new("organization_legal_business_name", DataType::Utf8, true),
            Field::new("organization_other_name", DataType::Utf8, true),
            Field::new("organization_other_name_type_code", DataType::Utf8, true),
            // Mailing Address
            Field::new("mailing_line_1", DataType::Utf8, true),
            Field::new("mailing_line_2", DataType::Utf8, true),
            Field::new("mailing_city", DataType::Utf8, true),
            Field::new("mailing_state", DataType::Utf8, true),
            Field::new("mailing_postal_code", DataType::Utf8, true),
            Field::new("mailing_country_code", DataType::Utf8, true),
            Field::new("mailing_telephone", DataType::Utf8, true),
            Field::new("mailing_fax", DataType::Utf8, true),
            // Practice Address
            Field::new("practice_line_1", DataType::Utf8, true),
            Field::new("practice_line_2", DataType::Utf8, true),
            Field::new("practice_city", DataType::Utf8, true),
            Field::new("practice_state", DataType::Utf8, true),
            Field::new("practice_postal_code", DataType::Utf8, true),
            Field::new("practice_country_code", DataType::Utf8, true),
            Field::new("practice_telephone", DataType::Utf8, true),
            Field::new("practice_fax", DataType::Utf8, true),
            // Dates
            Field::new("enumeration_date", DataType::Utf8, true),
            Field::new("last_update_date", DataType::Utf8, true),
            Field::new("deactivation_date", DataType::Utf8, true),
            Field::new("reactivation_date", DataType::Utf8, true),
            Field::new("certification_date", DataType::Utf8, true),
            // Status
            Field::new("deactivation_reason_code", DataType::Utf8, true),
            Field::new("provider_gender_code", DataType::Utf8, true),
            // Authorized Official
            Field::new("auth_official_name_prefix", DataType::Utf8, true),
            Field::new("auth_official_first_name", DataType::Utf8, true),
            Field::new("auth_official_middle_name", DataType::Utf8, true),
            Field::new("auth_official_last_name", DataType::Utf8, true),
            Field::new("auth_official_name_suffix", DataType::Utf8, true),
            Field::new("auth_official_credential", DataType::Utf8, true),
            Field::new("auth_official_title", DataType::Utf8, true),
            Field::new("auth_official_telephone", DataType::Utf8, true),
            // Taxonomy codes and other identifiers as JSON
            Field::new("taxonomy_codes_json", DataType::Utf8, true),
            Field::new("other_identifiers_json", DataType::Utf8, true),
            // Organization flags
            Field::new("is_sole_proprietor", DataType::Boolean, true),
            Field::new("is_organization_subpart", DataType::Boolean, true),
            Field::new("parent_organization_lbn", DataType::Utf8, true),
            Field::new("parent_organization_tin", DataType::Utf8, true),
        ]));
        // 2. Build Arrow arrays for each field
        let n = dataset.providers.len();
        macro_rules! col {
            ($getter:expr) => { (0..n).map($getter).collect::<StringArray>() };
        }
        macro_rules! col_opt {
            ($getter:expr) => { (0..n).map($getter).map(|v| v.as_deref().unwrap_or("")).collect::<StringArray>() };
        }
        macro_rules! col_date {
            ($getter:expr) => { (0..n).map($getter).map(|v| v.map(|d| d.to_string()).unwrap_or(String::new())).collect::<StringArray>() };
        }
        macro_rules! col_bool {
            ($getter:expr) => { (0..n).map($getter).collect::<BooleanArray>() };
        }
        let providers = &dataset.providers;
        let taxonomy_codes_json: StringArray = (0..n).map(|i| serde_json::to_string(&providers[i].taxonomy_codes).unwrap_or_default()).collect();
        let other_identifiers_json: StringArray = (0..n).map(|i| serde_json::to_string(&providers[i].other_identifiers).unwrap_or_default()).collect();
        let is_sole_proprietor: BooleanArray = (0..n).map(|i| providers[i].is_sole_proprietor.unwrap_or(false)).collect();
        let is_organization_subpart: BooleanArray = (0..n).map(|i| providers[i].is_organization_subpart.unwrap_or(false)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(col!(|i| providers[i].npi.as_str())),
                Arc::new(col!(|i| providers[i].entity_type.to_code())),
                Arc::new(col_opt!(|i| providers[i].replacement_npi.as_ref().map(|n| n.as_str().to_string()))),
                Arc::new(col_opt!(|i| providers[i].ein.as_ref().map(|s| s.clone()))),
                // ProviderName
                Arc::new(col_opt!(|i| providers[i].provider_name.prefix.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_name.first.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_name.middle.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_name.last.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_name.suffix.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_name.credential.as_ref().map(|s| s.clone()))),
                // ProviderOtherName
                Arc::new(col_opt!(|i| providers[i].provider_other_name.prefix.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name.first.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name.middle.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name.last.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name.suffix.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name.credential.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_other_name_type_code.as_ref().map(|s| s.clone()))),
                // OrganizationName
                Arc::new(col_opt!(|i| providers[i].organization_name.legal_business_name.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].organization_name.other_name.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].organization_name.other_name_type_code.as_ref().map(|s| s.clone()))),
                // Mailing Address
                Arc::new(col_opt!(|i| providers[i].mailing_address.line_1.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.line_2.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.city.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.state.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.postal_code.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.country_code.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.telephone.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].mailing_address.fax.as_ref().map(|s| s.clone()))),
                // Practice Address
                Arc::new(col_opt!(|i| providers[i].practice_address.line_1.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.line_2.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.city.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.state.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.postal_code.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.country_code.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.telephone.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].practice_address.fax.as_ref().map(|s| s.clone()))),
                // Dates
                Arc::new(col_date!(|i| providers[i].enumeration_date)),
                Arc::new(col_date!(|i| providers[i].last_update_date)),
                Arc::new(col_date!(|i| providers[i].deactivation_date)),
                Arc::new(col_date!(|i| providers[i].reactivation_date)),
                Arc::new(col_date!(|i| providers[i].certification_date)),
                // Status
                Arc::new(col_opt!(|i| providers[i].deactivation_reason_code.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].provider_gender_code.as_ref().map(|s| s.clone()))),
                // Authorized Official
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.name_prefix.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.first_name.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.middle_name.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.last_name.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.name_suffix.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.credential.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.title.clone()))),
                Arc::new(col_opt!(|i| providers[i].authorized_official.as_ref().and_then(|a| a.telephone.clone()))),
                // Taxonomy codes and other identifiers as JSON
                Arc::new(taxonomy_codes_json),
                Arc::new(other_identifiers_json),
                // Organization flags
                Arc::new(is_sole_proprietor),
                Arc::new(is_organization_subpart),
                Arc::new(col_opt!(|i| providers[i].parent_organization_lbn.as_ref().map(|s| s.clone()))),
                Arc::new(col_opt!(|i| providers[i].parent_organization_tin.as_ref().map(|s| s.clone()))),
            ],
        )?;
        // 3. Write to Parquet
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, Some(self.compression))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
    fn format(&self) -> ExportFormat {
        ExportFormat::Parquet
    }
}

// Export convenience functions for NppesDataset
impl NppesDataset {
    /// Export to JSON format
    pub fn export_json<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        JsonExporter::default().export(self, path.as_ref())
    }
    
    /// Export to JSON Lines format
    pub fn export_json_lines<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        JsonExporter::new()
            .as_json_lines()
            .export(self, path.as_ref())
    }
    
    /// Export to normalized CSV files
    pub fn export_csv<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        CsvExporter::default().export(self, path.as_ref())
    }
    
    /// Export to SQL insert statements
    pub fn export_sql<P: AsRef<Path>>(&self, path: P, dialect: SqlDialect) -> Result<()> {
        SqlExporter::new()
            .with_dialect(dialect)
            .export(self, path.as_ref())
    }
    
    /// Export a subset of providers
    pub fn export_subset<P: AsRef<Path>, F>(&self, path: P, filter: F, format: ExportFormat) -> Result<()>
    where
        F: Fn(&NppesRecord) -> bool,
    {
        // Create a temporary dataset with filtered providers
        let filtered_providers: Vec<NppesRecord> = self.providers.iter()
            .filter(|p| filter(p))
            .cloned()
            .collect();
        
        let subset = NppesDataset::new(
            filtered_providers,
            self.taxonomy_map.clone(),
            None,
            None,
            None,
            None, // npi_index
            None, // state_index
            None, // taxonomy_index
        );
        
        match format {
            ExportFormat::Json => JsonExporter::default().export(&subset, path.as_ref()),
            ExportFormat::Csv => CsvExporter::default().export(&subset, path.as_ref()),
            ExportFormat::Sql => SqlExporter::default().export(&subset, path.as_ref()),
            _ => Err(NppesError::Custom {
                message: format!("Export format {:?} not supported", format),
                suggestion: Some("Use JSON, CSV, or SQL format".to_string()),
            }),
        }
    }

    /// Export to Parquet format
    #[cfg(feature = "arrow-export")]
    pub fn export_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        ParquetExporter::default().export(self, path.as_ref())
    }

    #[cfg(feature = "arrow-export")]
    pub fn export_taxonomy_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let taxonomies: Vec<_> = self.taxonomy_map.as_ref().map(|m| m.values().cloned().collect()).unwrap_or_default();
        let n = taxonomies.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("grouping", DataType::Utf8, true),
            Field::new("classification", DataType::Utf8, true),
            Field::new("specialization", DataType::Utf8, true),
            Field::new("definition", DataType::Utf8, true),
            Field::new("notes", DataType::Utf8, true),
            Field::new("display_name", DataType::Utf8, true),
            Field::new("section", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new((0..n).map(|i| taxonomies[i].code.as_str()).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].grouping.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].classification.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].specialization.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].definition.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].notes.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].display_name.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| taxonomies[i].section.as_deref().unwrap_or("")).collect::<StringArray>()),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
    #[cfg(feature = "arrow-export")]
    pub fn export_other_names_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let other_names: Vec<_> = self.other_names_map.as_ref().map(|m| m.values().flatten().cloned().collect()).unwrap_or_default();
        let n = other_names.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("npi", DataType::Utf8, false),
            Field::new("provider_other_organization_name", DataType::Utf8, false),
            Field::new("provider_other_organization_name_type_code", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new((0..n).map(|i| other_names[i].npi.as_str()).collect::<StringArray>()),
                Arc::new((0..n).map(|i| other_names[i].provider_other_organization_name.as_str()).collect::<StringArray>()),
                Arc::new((0..n).map(|i| other_names[i].provider_other_organization_name_type_code.as_deref().unwrap_or("")).collect::<StringArray>()),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
    #[cfg(feature = "arrow-export")]
    pub fn export_practice_locations_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let locations: Vec<_> = self.practice_locations_map.as_ref().map(|m| m.values().flatten().cloned().collect()).unwrap_or_default();
        let n = locations.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("npi", DataType::Utf8, false),
            Field::new("address_json", DataType::Utf8, false),
            Field::new("telephone_extension", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new((0..n).map(|i| locations[i].npi.as_str()).collect::<StringArray>()),
                Arc::new((0..n).map(|i| address_to_json(&Some(locations[i].address.clone()))).collect::<StringArray>()),
                Arc::new((0..n).map(|i| locations[i].telephone_extension.as_deref().unwrap_or("")).collect::<StringArray>()),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
    #[cfg(feature = "arrow-export")]
    pub fn export_endpoints_parquet<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let endpoints: Vec<_> = self.endpoints_map.as_ref().map(|m| m.values().flatten().cloned().collect()).unwrap_or_default();
        let n = endpoints.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("npi", DataType::Utf8, false),
            Field::new("endpoint_type", DataType::Utf8, true),
            Field::new("endpoint_type_description", DataType::Utf8, true),
            Field::new("endpoint", DataType::Utf8, true),
            Field::new("affiliation", DataType::Boolean, true),
            Field::new("endpoint_description", DataType::Utf8, true),
            Field::new("affiliation_legal_business_name", DataType::Utf8, true),
            Field::new("use_code", DataType::Utf8, true),
            Field::new("use_description", DataType::Utf8, true),
            Field::new("other_use_description", DataType::Utf8, true),
            Field::new("content_type", DataType::Utf8, true),
            Field::new("content_description", DataType::Utf8, true),
            Field::new("other_content_description", DataType::Utf8, true),
            Field::new("affiliation_address_json", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new((0..n).map(|i| endpoints[i].npi.as_str()).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].endpoint_type.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].endpoint_type_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].endpoint.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].affiliation.unwrap_or(false)).collect::<BooleanArray>()),
                Arc::new((0..n).map(|i| endpoints[i].endpoint_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].affiliation_legal_business_name.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].use_code.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].use_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].other_use_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].content_type.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].content_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| endpoints[i].other_content_description.as_deref().unwrap_or("")).collect::<StringArray>()),
                Arc::new((0..n).map(|i| address_to_json(&endpoints[i].affiliation_address)).collect::<StringArray>()),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
}

#[cfg(feature = "arrow-export")]
fn address_to_json(addr: &Option<crate::data_types::Address>) -> String {
    serde_json::to_string(addr).unwrap_or_default()
}

#[cfg(feature = "arrow-export")]
fn address_from_json(s: &str) -> Option<crate::data_types::Address> {
    serde_json::from_str(s).ok()
}

#[cfg(feature = "arrow-export")]
impl NppesReader {
    #[cfg(feature = "arrow-export")]
    pub fn load_taxonomy_data_parquet<P: AsRef<Path>>(&self, path: P) -> Result<Vec<TaxonomyReference>> {
        use std::fs::File;
        use std::sync::Arc;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = File::open(path)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let record_batch_reader = arrow_reader.get_record_reader(1024)?;
        let mut records = Vec::new();
        for batch in record_batch_reader {
            let batch = batch?;
            let n = batch.num_rows();
            let col_str = |idx| batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..n {
                records.push(TaxonomyReference {
                    code: col_str(0).value(i).to_string(),
                    grouping: val_or_none(col_str(1).value(i)),
                    classification: val_or_none(col_str(2).value(i)),
                    specialization: val_or_none(col_str(3).value(i)),
                    definition: val_or_none(col_str(4).value(i)),
                    notes: val_or_none(col_str(5).value(i)),
                    display_name: val_or_none(col_str(6).value(i)),
                    section: val_or_none(col_str(7).value(i)),
                });
            }
        }
        Ok(records)
    }
    #[cfg(feature = "arrow-export")]
    pub fn load_other_name_data_parquet<P: AsRef<Path>>(&self, path: P) -> Result<Vec<OtherNameRecord>> {
        use std::fs::File;
        use std::sync::Arc;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = File::open(path)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let record_batch_reader = arrow_reader.get_record_reader(1024)?;
        let mut records = Vec::new();
        for batch in record_batch_reader {
            let batch = batch?;
            let n = batch.num_rows();
            let col_str = |idx| batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..n {
                records.push(OtherNameRecord {
                    npi: crate::data_types::Npi::new(col_str(0).value(i).to_string())?,
                    provider_other_organization_name: col_str(1).value(i).to_string(),
                    provider_other_organization_name_type_code: val_or_none(col_str(2).value(i)),
                });
            }
        }
        Ok(records)
    }
    #[cfg(feature = "arrow-export")]
    pub fn load_practice_location_data_parquet<P: AsRef<Path>>(&self, path: P) -> Result<Vec<PracticeLocationRecord>> {
        use std::fs::File;
        use std::sync::Arc;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = File::open(path)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let record_batch_reader = arrow_reader.get_record_reader(1024)?;
        let mut records = Vec::new();
        for batch in record_batch_reader {
            let batch = batch?;
            let n = batch.num_rows();
            let col_str = |idx| batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..n {
                records.push(PracticeLocationRecord {
                    npi: crate::data_types::Npi::new(col_str(0).value(i).to_string())?,
                    address: address_from_json(col_str(1).value(i)).unwrap_or_default(),
                    telephone_extension: val_or_none(col_str(2).value(i)),
                });
            }
        }
        Ok(records)
    }
    #[cfg(feature = "arrow-export")]
    pub fn load_endpoint_data_parquet<P: AsRef<Path>>(&self, path: P) -> Result<Vec<EndpointRecord>> {
        use std::fs::File;
        use std::sync::Arc;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let file = File::open(path)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let record_batch_reader = arrow_reader.get_record_reader(1024)?;
        let mut records = Vec::new();
        for batch in record_batch_reader {
            let batch = batch?;
            let n = batch.num_rows();
            let col_str = |idx| batch.column(idx).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            let col_bool = |idx| batch.column(idx).as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            for i in 0..n {
                records.push(EndpointRecord {
                    npi: crate::data_types::Npi::new(col_str(0).value(i).to_string())?,
                    endpoint_type: val_or_none(col_str(1).value(i)),
                    endpoint_type_description: val_or_none(col_str(2).value(i)),
                    endpoint: val_or_none(col_str(3).value(i)),
                    affiliation: if batch.column(4).is_null(i) { None } else { Some(col_bool(4).value(i)) },
                    endpoint_description: val_or_none(col_str(5).value(i)),
                    affiliation_legal_business_name: val_or_none(col_str(6).value(i)),
                    use_code: val_or_none(col_str(7).value(i)),
                    use_description: val_or_none(col_str(8).value(i)),
                    other_use_description: val_or_none(col_str(9).value(i)),
                    content_type: val_or_none(col_str(10).value(i)),
                    content_description: val_or_none(col_str(11).value(i)),
                    other_content_description: val_or_none(col_str(12).value(i)),
                    affiliation_address: address_from_json(col_str(13).value(i)),
                });
            }
        }
        Ok(records)
    }
}

#[cfg(feature = "arrow-export")]
fn val_or_none(s: &str) -> Option<String> {
    if s.is_empty() { None } else { Some(s.to_string()) }
}

#[cfg(feature = "arrow-export")]
fn parse_date_opt(s: &str) -> Option<chrono::NaiveDate> {
    if s.is_empty() { None } else { chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok() }
} 