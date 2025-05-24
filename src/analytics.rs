/*!
 * Analytics and querying functionality for NPPES data
 * 
 * This module provides tools for analyzing and querying NPPES provider data,
 * including filtering, aggregation, and statistical analysis functions.
 */

use std::collections::{HashMap, HashSet};
use chrono::NaiveDate;
use std::sync::Arc;

use crate::{
    Result, NppesError,
    data_types::*,
};

/// Analytics engine for NPPES data analysis
pub struct NppesAnalytics<'a> {
    /// Main provider records
    providers: &'a [NppesRecord],
    /// Taxonomy reference data
    taxonomy_ref: Option<&'a [TaxonomyReference]>,
    /// Other name records
    other_names: Option<&'a [OtherNameRecord]>,
    /// Practice location records
    practice_locations: Option<&'a [PracticeLocationRecord]>,
    /// Endpoint records
    endpoints: Option<&'a [EndpointRecord]>,
}

impl<'a> NppesAnalytics<'a> {
    /// Create a new analytics engine with provider data
    pub fn new(providers: &'a [NppesRecord]) -> Self {
        Self {
            providers,
            taxonomy_ref: None,
            other_names: None,
            practice_locations: None,
            endpoints: None,
        }
    }
    
    /// Add taxonomy reference data for enrichment
    pub fn with_taxonomy_reference(mut self, taxonomy_ref: &'a [TaxonomyReference]) -> Self {
        self.taxonomy_ref = Some(taxonomy_ref);
        self
    }
    
    /// Add other name reference data
    pub fn with_other_names(mut self, other_names: &'a [OtherNameRecord]) -> Self {
        self.other_names = Some(other_names);
        self
    }
    
    /// Add practice location reference data
    pub fn with_practice_locations(mut self, practice_locations: &'a [PracticeLocationRecord]) -> Self {
        self.practice_locations = Some(practice_locations);
        self
    }
    
    /// Add endpoint reference data
    pub fn with_endpoints(mut self, endpoints: &'a [EndpointRecord]) -> Self {
        self.endpoints = Some(endpoints);
        self
    }
    
    /// Get basic statistics about the provider dataset
    pub fn dataset_stats(&self) -> DatasetStats {
        let total_providers = self.providers.len();
        let individual_count = self.providers.iter()
            .filter(|p| p.entity_type == Some(EntityType::Individual))
            .count();
        let organization_count = total_providers - individual_count;
        
        let active_count = self.providers.iter()
            .filter(|p| p.is_active())
            .count();
        let inactive_count = total_providers - active_count;
        
        let unique_states = self.providers.iter()
            .filter_map(|p| p.mailing_address.state.as_ref())
            .map(|s| s.as_code())
            .collect::<HashSet<_>>()
            .len();
        
        let unique_taxonomy_codes = self.providers.iter()
            .flat_map(|p| &p.taxonomy_codes)
            .map(|t| &t.code)
            .collect::<HashSet<_>>()
            .len();
        
        DatasetStats {
            total_providers,
            individual_providers: individual_count,
            organization_providers: organization_count,
            active_providers: active_count,
            inactive_providers: inactive_count,
            unique_states,
            unique_taxonomy_codes,
        }
    }
    
    /// Find providers by NPI
    pub fn find_by_npi(&self, npi: &Npi) -> Option<&NppesRecord> {
        self.providers.iter().find(|p| &p.npi == npi)
    }
    
    /// Find providers by name (partial match)
    pub fn find_by_name(&self, name_query: &str) -> Vec<&NppesRecord> {
        let query_lower = name_query.to_lowercase();
        
        self.providers.iter()
            .filter(|p| {
                let display_name = p.display_name().to_lowercase();
                display_name.contains(&query_lower)
            })
            .collect()
    }
    
    /// Find providers by state
    pub fn find_by_state(&self, state: &str) -> Vec<&NppesRecord> {
        let state_enum = StateCode::from_code(state);
        self.providers.iter()
            .filter(|p| {
                p.mailing_address.state.as_ref()
                    .map(|s| Some(s) == state_enum.as_ref())
                    .unwrap_or(false)
            })
            .collect()
    }
    
    /// Find providers by taxonomy code
    pub fn find_by_taxonomy_code(&self, taxonomy_code: &str) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| {
                p.taxonomy_codes.iter().any(|t| t.code == taxonomy_code)
            })
            .collect()
    }
    
    /// Find providers by entity type
    pub fn find_by_entity_type(&self, entity_type: EntityType) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| p.entity_type.as_ref() == Some(&entity_type))
            .collect()
    }
    
    /// Get provider count by state
    pub fn provider_count_by_state(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        
        for provider in self.providers {
            if let Some(state) = &provider.mailing_address.state {
                *counts.entry(state.as_code().to_string()).or_insert(0) += 1;
            }
        }
        
        counts
    }
    
    /// Get provider count by taxonomy code
    pub fn provider_count_by_taxonomy(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        
        for provider in self.providers {
            for taxonomy in &provider.taxonomy_codes {
                *counts.entry(taxonomy.code.clone()).or_insert(0) += 1;
            }
        }
        
        counts
    }
    
    /// Get provider count by entity type
    pub fn provider_count_by_entity_type(&self) -> HashMap<EntityType, usize> {
        let mut counts = HashMap::new();
        
        for provider in self.providers {
            if let Some(ref entity_type) = provider.entity_type {
                *counts.entry(entity_type.clone()).or_insert(0) += 1;
            }
        }
        
        counts
    }
    
    /// Get top N states by provider count
    pub fn top_states_by_provider_count(&self, limit: usize) -> Vec<(String, usize)> {
        let mut state_counts: Vec<_> = self.provider_count_by_state().into_iter().collect();
        state_counts.sort_by(|a, b| b.1.cmp(&a.1));
        state_counts.truncate(limit);
        state_counts
    }
    
    /// Get top N taxonomy codes by provider count
    pub fn top_taxonomy_codes_by_provider_count(&self, limit: usize) -> Vec<(String, usize)> {
        let mut taxonomy_counts: Vec<_> = self.provider_count_by_taxonomy().into_iter().collect();
        taxonomy_counts.sort_by(|a, b| b.1.cmp(&a.1));
        taxonomy_counts.truncate(limit);
        taxonomy_counts
    }
    
    /// Get providers enumerated in a date range
    pub fn providers_enumerated_between(&self, start_date: NaiveDate, end_date: NaiveDate) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| {
                if let Some(enum_date) = p.enumeration_date {
                    enum_date >= start_date && enum_date <= end_date
                } else {
                    false
                }
            })
            .collect()
    }
    
    /// Get providers updated in a date range
    pub fn providers_updated_between(&self, start_date: NaiveDate, end_date: NaiveDate) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| {
                if let Some(update_date) = p.last_update_date {
                    update_date >= start_date && update_date <= end_date
                } else {
                    false
                }
            })
            .collect()
    }
    
    /// Get active providers only
    pub fn active_providers(&self) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| p.is_active())
            .collect()
    }
    
    /// Get inactive providers only
    pub fn inactive_providers(&self) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| !p.is_active())
            .collect()
    }
    
    /// Find providers with primary taxonomy code
    pub fn providers_with_primary_taxonomy(&self) -> Vec<&NppesRecord> {
        self.providers.iter()
            .filter(|p| p.primary_taxonomy().is_some())
            .collect()
    }
    
    /// Get taxonomy description for a code (requires taxonomy reference data)
    pub fn get_taxonomy_description(&self, taxonomy_code: &str) -> Option<&TaxonomyReference> {
        self.taxonomy_ref?
            .iter()
            .find(|t| t.code == taxonomy_code)
    }
    
    /// Enrich providers with taxonomy descriptions
    pub fn enrich_with_taxonomy_descriptions(&self) -> Result<Vec<EnrichedProvider>> {
        if self.taxonomy_ref.is_none() {
            return Err(NppesError::DataValidation {
                message: "Taxonomy reference data required for enrichment".to_string(),
                field: None,
                value: None,
                context: Default::default(),
            });
        }
        
        let taxonomy_map: HashMap<&str, &TaxonomyReference> = self.taxonomy_ref.unwrap()
            .iter()
            .map(|t| (t.code.as_str(), t))
            .collect();
        
        let mut enriched_providers = Vec::new();
        
        for provider in self.providers {
            let enriched_taxonomies: Vec<_> = provider.taxonomy_codes.iter()
                .map(|tc| {
                    let taxonomy_ref = taxonomy_map.get(tc.code.as_str());
                    EnrichedTaxonomyCode {
                        code: tc.code.clone(),
                        license_number: tc.license_number.clone(),
                        license_state: tc.license_state.clone(),
                        is_primary: tc.is_primary,
                        taxonomy_group: tc.taxonomy_group.clone(),
                        display_name: taxonomy_ref.and_then(|t| t.display_name.clone()),
                        classification: taxonomy_ref.and_then(|t| t.classification.clone()),
                        specialization: taxonomy_ref.and_then(|t| t.specialization.clone()),
                    }
                })
                .collect();
            
            enriched_providers.push(EnrichedProvider {
                provider: provider.clone(),
                enriched_taxonomies,
            });
        }
        
        Ok(enriched_providers)
    }
    
    /// Create a provider lookup index by NPI for fast access
    pub fn create_npi_index(&self) -> HashMap<&Npi, &NppesRecord> {
        self.providers.iter()
            .map(|p| (&p.npi, p))
            .collect()
    }
    
    /// Create a provider lookup index by state
    pub fn create_state_index(&self) -> HashMap<String, Vec<&NppesRecord>> {
        let mut index = HashMap::new();
        
        for provider in self.providers {
            if let Some(state) = &provider.mailing_address.state {
                index.entry(state.as_code().to_string())
                    .or_insert_with(Vec::new)
                    .push(provider);
            }
        }
        
        index
    }
}

/// Statistics about the NPPES dataset
#[derive(Debug, Clone)]
pub struct DatasetStats {
    pub total_providers: usize,
    pub individual_providers: usize,
    pub organization_providers: usize,
    pub active_providers: usize,
    pub inactive_providers: usize,
    pub unique_states: usize,
    pub unique_taxonomy_codes: usize,
}

impl DatasetStats {
    /// Print formatted statistics
    pub fn print_summary(&self) {
        println!("=== NPPES Dataset Statistics ===");
        println!("Total Providers: {}", self.total_providers);
        println!("  Individual Providers: {}", self.individual_providers);
        println!("  Organization Providers: {}", self.organization_providers);
        println!("Active Providers: {}", self.active_providers);
        println!("Inactive Providers: {}", self.inactive_providers);
        println!("Unique States: {}", self.unique_states);
        println!("Unique Taxonomy Codes: {}", self.unique_taxonomy_codes);
        
        if self.total_providers > 0 {
            let individual_percent = (self.individual_providers as f64 / self.total_providers as f64) * 100.0;
            let active_percent = (self.active_providers as f64 / self.total_providers as f64) * 100.0;
            println!("Individual Provider Percentage: {:.1}%", individual_percent);
            println!("Active Provider Percentage: {:.1}%", active_percent);
        }
    }
}

/// Provider record enriched with taxonomy descriptions
#[derive(Debug, Clone)]
pub struct EnrichedProvider {
    pub provider: NppesRecord,
    pub enriched_taxonomies: Vec<EnrichedTaxonomyCode>,
}

/// Taxonomy code enriched with human-readable descriptions
#[derive(Debug, Clone)]
pub struct EnrichedTaxonomyCode {
    pub code: String,
    pub license_number: Option<String>,
    pub license_state: Option<String>,
    pub is_primary: bool,
    pub taxonomy_group: Option<String>,
    pub display_name: Option<String>,
    pub classification: Option<String>,
    pub specialization: Option<String>,
}

/// Query builder for complex provider searches
pub struct ProviderQuery<'a> {
    analytics: &'a NppesAnalytics<'a>,
    filters: Vec<Box<dyn Fn(&NppesRecord) -> bool + 'a>>,
}

impl<'a> ProviderQuery<'a> {
    /// Create a new query builder
    pub fn new(analytics: &'a NppesAnalytics<'a>) -> Self {
        Self {
            analytics,
            filters: Vec::new(),
        }
    }
    
    /// Filter by entity type
    pub fn entity_type(mut self, entity_type: EntityType) -> Self {
        let entity_type = Arc::new(entity_type);
        self.filters.push(Box::new(move |p| p.entity_type.as_ref() == Some(&*entity_type)));
        self
    }
    
    /// Filter by state
    pub fn state<S: AsRef<str> + 'a>(mut self, state: S) -> Self {
        let state_enum = StateCode::from_code(state.as_ref());
        self.filters.push(Box::new(move |p| {
            p.mailing_address.state.as_ref()
                .map(|s| Some(s) == state_enum.as_ref())
                .unwrap_or(false)
        }));
        self
    }
    
    /// Filter by taxonomy code
    pub fn taxonomy_code<S: AsRef<str> + 'a>(mut self, taxonomy_code: S) -> Self {
        let taxonomy_code = taxonomy_code.as_ref().to_string();
        self.filters.push(Box::new(move |p| {
            p.taxonomy_codes.iter().any(|t| t.code == taxonomy_code)
        }));
        self
    }
    
    /// Filter by active status
    pub fn active_only(mut self) -> Self {
        self.filters.push(Box::new(|p| p.is_active()));
        self
    }
    
    /// Filter by inactive status
    pub fn inactive_only(mut self) -> Self {
        self.filters.push(Box::new(|p| !p.is_active()));
        self
    }
    
    /// Filter by enumeration date range
    pub fn enumerated_between(mut self, start_date: NaiveDate, end_date: NaiveDate) -> Self {
        self.filters.push(Box::new(move |p| {
            if let Some(enum_date) = p.enumeration_date {
                enum_date >= start_date && enum_date <= end_date
            } else {
                false
            }
        }));
        self
    }
    
    /// Execute the query and return matching providers
    pub fn execute(self) -> Vec<&'a NppesRecord> {
        self.analytics.providers.iter()
            .filter(|provider| {
                self.filters.iter().all(|filter| filter(provider))
            })
            .collect()
    }
    
    /// Execute the query and return count only
    pub fn count(self) -> usize {
        self.execute().len()
    }
} 