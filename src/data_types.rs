/*!
 * Data type definitions for NPPES records
 * 
 * This module contains type-safe representations of all NPPES data structures
 * based on the official NPPES Data Dissemination documentation.
 */

use serde::{Deserialize, Serialize};
use chrono::NaiveDate;
use crate::constants::{MAX_TAXONOMY_CODES, MAX_OTHER_IDENTIFIERS};

/// NPI (National Provider Identifier) - 10 digit unique identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Npi(pub String);

impl Npi {
    /// Create a new NPI, validating format
    pub fn new(npi: String) -> Result<Self, crate::NppesError> {
        if npi.len() != 10 || !npi.chars().all(|c| c.is_ascii_digit()) {
            return Err(crate::NppesError::invalid_npi(&npi));
        }
        Ok(Npi(npi))
    }
    
    /// Get the NPI as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Npi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Entity Type Code (1 = Individual, 2 = Organization)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Individual,
    Organization,
}

impl EntityType {
    pub fn from_code(code: &str) -> Result<Self, crate::NppesError> {
        match code {
            "1" => Ok(EntityType::Individual),
            "2" => Ok(EntityType::Organization),
            _ => Err(crate::NppesError::invalid_entity_type(code)),
        }
    }
    
    pub fn to_code(&self) -> &str {
        match self {
            EntityType::Individual => "1",
            EntityType::Organization => "2",
        }
    }
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityType::Individual => write!(f, "Individual"),
            EntityType::Organization => write!(f, "Organization"),
        }
    }
}

/// Healthcare Provider Taxonomy Code information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaxonomyCode {
    pub code: String,
    pub license_number: Option<String>,
    pub license_state: Option<String>,
    pub is_primary: bool,
    pub taxonomy_group: Option<String>,
}

/// Other Provider Identifier information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtherIdentifier {
    pub identifier: String,
    pub type_code: Option<String>,
    pub state: Option<String>,
    pub issuer: Option<String>,
}

/// Address information (mailing or practice location)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Address {
    pub line_1: Option<String>,
    pub line_2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country_code: Option<String>,
    pub telephone: Option<String>,
    pub fax: Option<String>,
}

impl Address {
    /// Check if the address is empty
    pub fn is_empty(&self) -> bool {
        self.line_1.is_none() 
            && self.line_2.is_none()
            && self.city.is_none()
            && self.state.is_none()
            && self.postal_code.is_none()
    }
    
    /// Format as a single line address
    pub fn format_single_line(&self) -> String {
        let mut parts = Vec::new();
        
        if let Some(line1) = &self.line_1 {
            parts.push(line1.clone());
        }
        if let Some(city) = &self.city {
            parts.push(city.clone());
        }
        if let Some(state) = &self.state {
            parts.push(state.clone());
        }
        if let Some(zip) = &self.postal_code {
            parts.push(zip.clone());
        }
        
        parts.join(", ")
    }
}

/// Provider name information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderName {
    pub prefix: Option<String>,
    pub first: Option<String>,
    pub middle: Option<String>,
    pub last: Option<String>,
    pub suffix: Option<String>,
    pub credential: Option<String>,
}

impl ProviderName {
    /// Format the full name
    pub fn full_name(&self) -> String {
        let mut parts = Vec::new();
        
        if let Some(prefix) = &self.prefix {
            parts.push(prefix.clone());
        }
        if let Some(first) = &self.first {
            parts.push(first.clone());
        }
        if let Some(middle) = &self.middle {
            parts.push(middle.clone());
        }
        if let Some(last) = &self.last {
            parts.push(last.clone());
        }
        if let Some(suffix) = &self.suffix {
            parts.push(suffix.clone());
        }
        if let Some(credential) = &self.credential {
            parts.push(format!("({})", credential));
        }
        
        parts.join(" ")
    }
}

/// Organization name information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrganizationName {
    pub legal_business_name: Option<String>,
    pub other_name: Option<String>,
    pub other_name_type_code: Option<String>,
}

/// Authorized Official information (for organizations)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthorizedOfficial {
    pub name_prefix: Option<String>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    pub name_suffix: Option<String>,
    pub credential: Option<String>,
    pub title: Option<String>,
    pub telephone: Option<String>,
}

impl AuthorizedOfficial {
    /// Format the full name
    pub fn full_name(&self) -> String {
        let mut parts = Vec::new();
        
        if let Some(prefix) = &self.name_prefix {
            parts.push(prefix.clone());
        }
        if let Some(first) = &self.first_name {
            parts.push(first.clone());
        }
        if let Some(middle) = &self.middle_name {
            parts.push(middle.clone());
        }
        if let Some(last) = &self.last_name {
            parts.push(last.clone());
        }
        if let Some(suffix) = &self.name_suffix {
            parts.push(suffix.clone());
        }
        if let Some(credential) = &self.credential {
            parts.push(format!("({})", credential));
        }
        
        parts.join(" ")
    }
}

/// Main NPPES Provider Record
/// 
/// This struct represents the main provider data from the NPPES CSV file.
/// It contains all 330+ columns as defined in the NPPES documentation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NppesRecord {
    // Core identifiers
    pub npi: Npi,
    pub entity_type: EntityType,
    pub replacement_npi: Option<Npi>,
    pub ein: Option<String>,
    
    // Provider names
    pub provider_name: ProviderName,
    pub provider_other_name: ProviderName,
    pub provider_other_name_type_code: Option<String>,
    
    // Organization information
    pub organization_name: OrganizationName,
    
    // Addresses
    pub mailing_address: Address,
    pub practice_address: Address,
    
    // Dates
    pub enumeration_date: Option<NaiveDate>,
    pub last_update_date: Option<NaiveDate>,
    pub deactivation_date: Option<NaiveDate>,
    pub reactivation_date: Option<NaiveDate>,
    pub certification_date: Option<NaiveDate>,
    
    // Status information
    pub deactivation_reason_code: Option<String>,
    pub provider_gender_code: Option<String>,
    
    // Authorized official (for organizations)
    pub authorized_official: Option<AuthorizedOfficial>,
    
    // Healthcare taxonomy codes (up to 15)
    pub taxonomy_codes: Vec<TaxonomyCode>,
    
    // Other provider identifiers (up to 50)
    pub other_identifiers: Vec<OtherIdentifier>,
    
    // Organization flags
    pub is_sole_proprietor: Option<bool>,
    pub is_organization_subpart: Option<bool>,
    pub parent_organization_lbn: Option<String>,
    pub parent_organization_tin: Option<String>,
}

impl NppesRecord {
    /// Get the primary taxonomy code
    pub fn primary_taxonomy(&self) -> Option<&TaxonomyCode> {
        self.taxonomy_codes.iter().find(|t| t.is_primary)
    }
    
    /// Get all taxonomy codes
    pub fn all_taxonomy_codes(&self) -> &[TaxonomyCode] {
        &self.taxonomy_codes
    }
    
    /// Check if provider is active (not deactivated)
    pub fn is_active(&self) -> bool {
        self.deactivation_date.is_none()
    }
    
    /// Get provider's primary name based on entity type
    pub fn display_name(&self) -> String {
        match self.entity_type {
            EntityType::Individual => {
                let name = &self.provider_name;
                format!("{} {}", 
                    name.first.as_deref().unwrap_or(""),
                    name.last.as_deref().unwrap_or("")
                ).trim().to_string()
            },
            EntityType::Organization => {
                self.organization_name.legal_business_name
                    .as_deref()
                    .unwrap_or("Unknown Organization")
                    .to_string()
            }
        }
    }
    
    /// Get full formatted name (includes credentials and titles)
    pub fn full_display_name(&self) -> String {
        match self.entity_type {
            EntityType::Individual => self.provider_name.full_name(),
            EntityType::Organization => {
                self.organization_name.legal_business_name
                    .as_deref()
                    .unwrap_or("Unknown Organization")
                    .to_string()
            }
        }
    }
}

/// Other Name Reference record
/// 
/// Contains additional organization names for Type 2 NPIs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtherNameRecord {
    pub npi: Npi,
    pub provider_other_organization_name: String,
    pub provider_other_organization_name_type_code: Option<String>,
}

/// Practice Location Reference record
/// 
/// Contains non-primary practice locations for providers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PracticeLocationRecord {
    pub npi: Npi,
    pub address: Address,
    pub telephone_extension: Option<String>,
}

/// Endpoint Reference record
/// 
/// Contains healthcare endpoints associated with NPIs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointRecord {
    pub npi: Npi,
    pub endpoint_type: Option<String>,
    pub endpoint_type_description: Option<String>,
    pub endpoint: Option<String>,
    pub affiliation: Option<bool>,
    pub endpoint_description: Option<String>,
    pub affiliation_legal_business_name: Option<String>,
    pub use_code: Option<String>,
    pub use_description: Option<String>,
    pub other_use_description: Option<String>,
    pub content_type: Option<String>,
    pub content_description: Option<String>,
    pub other_content_description: Option<String>,
    pub affiliation_address: Option<Address>,
}

/// Healthcare Provider Taxonomy Reference
/// 
/// Reference data for taxonomy codes from NUCC
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaxonomyReference {
    pub code: String,
    pub grouping: Option<String>,
    pub classification: Option<String>,
    pub specialization: Option<String>,
    pub definition: Option<String>,
    pub notes: Option<String>,
    pub display_name: Option<String>,
    pub section: Option<String>,
} 