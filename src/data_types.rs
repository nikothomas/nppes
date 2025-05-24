/*!
 * Data type definitions for NPPES records
 * 
 * This module contains type-safe representations of all NPPES data structures
 * based on the official NPPES Data Dissemination documentation.
 */

use serde::{Deserialize, Serialize};
use chrono::NaiveDate;

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

pub trait OptionDisplay {
    fn option_display(&self) -> String;
}

impl OptionDisplay for Option<EntityType> {
    fn option_display(&self) -> String {
        match self {
            Some(entity_type) => entity_type.to_string(),
            None => String::new(),
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
    /// Canonical 2025 group taxonomy code (required)
    pub group_taxonomy_code: Option<GroupTaxonomyCode>,
    /// Canonical 2025 primary taxonomy switch (required)
    pub primary_switch: Option<PrimaryTaxonomySwitch>,
}

/// Other Provider Identifier information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtherIdentifier {
    pub identifier: String,
    pub type_code: Option<String>,
    /// Canonical 2025 issuer code (required)
    pub issuer: Option<OtherProviderIdentifierIssuerCode>,
    /// Canonical 2025 state code (required)
    pub state: Option<StateCode>,
}

/// Address information (mailing or practice location)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Address {
    pub line_1: Option<String>,
    pub line_2: Option<String>,
    pub city: Option<String>,
    pub postal_code: Option<String>,
    pub telephone: Option<String>,
    pub fax: Option<String>,
    /// Canonical 2025 state code (required)
    pub state: Option<StateCode>,
    /// Canonical 2025 country code (required)
    pub country: Option<CountryCode>,
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
        if let Some(state) = self.state.as_ref().map(|s| s.as_code().to_string()) {
            parts.push(state);
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
    pub prefix: Option<NamePrefixCode>,
    pub first: Option<String>,
    pub middle: Option<String>,
    pub last: Option<String>,
    pub suffix: Option<NameSuffixCode>,
    pub credential: Option<String>,
}

impl ProviderName {
    /// Format the full name
    pub fn full_name(&self) -> String {
        let mut parts = Vec::new();
        
        if let Some(prefix) = &self.prefix {
            parts.push(prefix.as_code().to_string());
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
            parts.push(suffix.as_code().to_string());
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
    /// Canonical 2025 other name type code (required)
    pub other_name_type: Option<OtherProviderNameTypeCode>,
}

/// Authorized Official information (for organizations)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthorizedOfficial {
    pub prefix: Option<NamePrefixCode>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    pub suffix: Option<NameSuffixCode>,
    pub credential: Option<String>,
    pub title: Option<String>,
    pub telephone: Option<String>,
}

impl AuthorizedOfficial {
    /// Format the full name
    pub fn full_name(&self) -> String {
        let mut parts = Vec::new();
        
        if let Some(prefix) = &self.prefix {
            parts.push(prefix.as_code().to_string());
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
        if let Some(suffix) = &self.suffix {
            parts.push(suffix.as_code().to_string());
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
    pub entity_type: Option<EntityType>,
    pub replacement_npi: Option<Npi>,
    pub ein: Option<String>,
    
    // Provider names
    pub provider_name: ProviderName,
    pub provider_other_name: ProviderName,
    /// Canonical 2025 other name type code (required)
    pub provider_other_name_type: Option<OtherProviderNameTypeCode>,
    
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
    /// Canonical 2025 deactivation reason code (required)
    pub deactivation_reason: Option<DeactivationReasonCode>,
    /// Canonical 2025 sex code (required)
    pub provider_gender: Option<SexCode>,
    
    // Authorized official (for organizations)
    pub authorized_official: Option<AuthorizedOfficial>,
    
    // Healthcare taxonomy codes (up to 15)
    pub taxonomy_codes: Vec<TaxonomyCode>,
    
    // Other provider identifiers (up to 50)
    pub other_identifiers: Vec<OtherIdentifier>,
    
    // Organization flags
    /// Canonical 2025 sole proprietor code (required)
    pub sole_proprietor: Option<SoleProprietorCode>,
    /// Canonical 2025 subpart code (required)
    pub organization_subpart: Option<SubpartCode>,
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
            Some(EntityType::Individual) => {
                let name = &self.provider_name;
                format!("{} {}", 
                    name.first.as_deref().unwrap_or(""),
                    name.last.as_deref().unwrap_or("")
                ).trim().to_string()
            },
            Some(EntityType::Organization) => {
                self.organization_name.legal_business_name
                    .as_deref()
                    .unwrap_or("Unknown Organization")
                    .to_string()
            },
            None => "Unknown".to_string(),
        }
    }
    
    /// Get full formatted name (includes credentials and titles)
    pub fn full_display_name(&self) -> String {
        match self.entity_type {
            Some(EntityType::Individual) => self.provider_name.full_name(),
            Some(EntityType::Organization) => {
                self.organization_name.legal_business_name
                    .as_deref()
                    .unwrap_or("Unknown Organization")
                    .to_string()
            },
            None => "Unknown".to_string(),
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

/// Sole Proprietor Code (X, Y, N)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SoleProprietorCode {
    NotAnswered, // X
    Yes,         // Y
    No,          // N
}

impl SoleProprietorCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "X" => Some(SoleProprietorCode::NotAnswered),
            "Y" => Some(SoleProprietorCode::Yes),
            "N" => Some(SoleProprietorCode::No),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            SoleProprietorCode::NotAnswered => "X",
            SoleProprietorCode::Yes => "Y",
            SoleProprietorCode::No => "N",
        }
    }
}

/// Subpart Code (X, Y, N)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubpartCode {
    NotAnswered, // X
    Yes,         // Y
    No,          // N
}

impl SubpartCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "X" => Some(SubpartCode::NotAnswered),
            "Y" => Some(SubpartCode::Yes),
            "N" => Some(SubpartCode::No),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            SubpartCode::NotAnswered => "X",
            SubpartCode::Yes => "Y",
            SubpartCode::No => "N",
        }
    }
}

/// Sex Code (M, F, U, X)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SexCode {
    Male,        // M
    Female,      // F
    Undisclosed, // U or X
}

impl SexCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "M" => Some(SexCode::Male),
            "F" => Some(SexCode::Female),
            "U" | "X" => Some(SexCode::Undisclosed),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            SexCode::Male => "M",
            SexCode::Female => "F",
            SexCode::Undisclosed => "U", // Prefer U for output
        }
    }
}

/// Deactivation Reason Code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeactivationReasonCode {
    Death,
    Disbandment,
    Fraud,
    Other,
    Undisclosed,
}

impl DeactivationReasonCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code.to_ascii_lowercase().as_str() {
            "death" => Some(DeactivationReasonCode::Death),
            "disbandment" => Some(DeactivationReasonCode::Disbandment),
            "fraud" => Some(DeactivationReasonCode::Fraud),
            "other" => Some(DeactivationReasonCode::Other),
            "u" | "undisclosed" | "x" => Some(DeactivationReasonCode::Undisclosed),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            DeactivationReasonCode::Death => "Death",
            DeactivationReasonCode::Disbandment => "Disbandment",
            DeactivationReasonCode::Fraud => "Fraud",
            DeactivationReasonCode::Other => "Other",
            DeactivationReasonCode::Undisclosed => "Undisclosed",
        }
    }
}

/// Other Provider Name Type Code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OtherProviderNameTypeCode {
    FormerName,         // 1
    ProfessionalName,   // 2
    DoingBusinessAs,    // 3
    FormerLegalBusinessName, // 4
    OtherName,          // 5
}

impl OtherProviderNameTypeCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "1" => Some(OtherProviderNameTypeCode::FormerName),
            "2" => Some(OtherProviderNameTypeCode::ProfessionalName),
            "3" => Some(OtherProviderNameTypeCode::DoingBusinessAs),
            "4" => Some(OtherProviderNameTypeCode::FormerLegalBusinessName),
            "5" => Some(OtherProviderNameTypeCode::OtherName),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            OtherProviderNameTypeCode::FormerName => "1",
            OtherProviderNameTypeCode::ProfessionalName => "2",
            OtherProviderNameTypeCode::DoingBusinessAs => "3",
            OtherProviderNameTypeCode::FormerLegalBusinessName => "4",
            OtherProviderNameTypeCode::OtherName => "5",
        }
    }
}

/// Name Prefix Code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NamePrefixCode {
    Ms,
    Mr,
    Miss,
    Mrs,
    Dr,
    Prof,
}

impl NamePrefixCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "Ms." => Some(NamePrefixCode::Ms),
            "Mr." => Some(NamePrefixCode::Mr),
            "Miss" => Some(NamePrefixCode::Miss),
            "Mrs." => Some(NamePrefixCode::Mrs),
            "Dr." => Some(NamePrefixCode::Dr),
            "Prof." => Some(NamePrefixCode::Prof),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            NamePrefixCode::Ms => "Ms.",
            NamePrefixCode::Mr => "Mr.",
            NamePrefixCode::Miss => "Miss",
            NamePrefixCode::Mrs => "Mrs.",
            NamePrefixCode::Dr => "Dr.",
            NamePrefixCode::Prof => "Prof.",
        }
    }
}

/// Name Suffix Code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NameSuffixCode {
    Jr,
    Sr,
    I,
    II,
    III,
    IV,
    V,
    VI,
    VII,
    VIII,
    IX,
    X,
}

impl NameSuffixCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "Jr." => Some(NameSuffixCode::Jr),
            "Sr." => Some(NameSuffixCode::Sr),
            "I" => Some(NameSuffixCode::I),
            "II" => Some(NameSuffixCode::II),
            "III" => Some(NameSuffixCode::III),
            "IV" => Some(NameSuffixCode::IV),
            "V" => Some(NameSuffixCode::V),
            "VI" => Some(NameSuffixCode::VI),
            "VII" => Some(NameSuffixCode::VII),
            "VIII" => Some(NameSuffixCode::VIII),
            "IX" => Some(NameSuffixCode::IX),
            "X" => Some(NameSuffixCode::X),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            NameSuffixCode::Jr => "Jr.",
            NameSuffixCode::Sr => "Sr.",
            NameSuffixCode::I => "I",
            NameSuffixCode::II => "II",
            NameSuffixCode::III => "III",
            NameSuffixCode::IV => "IV",
            NameSuffixCode::V => "V",
            NameSuffixCode::VI => "VI",
            NameSuffixCode::VII => "VII",
            NameSuffixCode::VIII => "VIII",
            NameSuffixCode::IX => "IX",
            NameSuffixCode::X => "X",
        }
    }
}

/// State Code (US states, territories, ZZ)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StateCode {
    AK, AL, AR, AS, AZ, CA, CO, CT, DC, DE, FL, FM, GA, GU, HI, IA, ID, IL, IN, KS, KY, LA, MA, MD, ME, MH, MI, MN, MO, MP, MS, MT, NC, ND, NE, NH, NJ, NM, NV, NY, OH, OK, OR, PA, PR, PW, RI, SC, SD, TN, TX, UT, VA, VI, VT, WA, WI, WV, WY, ZZ,
}

impl StateCode {
    pub fn from_code(code: &str) -> Option<Self> {
        use StateCode::*;
        match code.to_ascii_uppercase().as_str() {
            "AK" => Some(AK), "AL" => Some(AL), "AR" => Some(AR), "AS" => Some(AS), "AZ" => Some(AZ),
            "CA" => Some(CA), "CO" => Some(CO), "CT" => Some(CT), "DC" => Some(DC), "DE" => Some(DE),
            "FL" => Some(FL), "FM" => Some(FM), "GA" => Some(GA), "GU" => Some(GU), "HI" => Some(HI),
            "IA" => Some(IA), "ID" => Some(ID), "IL" => Some(IL), "IN" => Some(IN), "KS" => Some(KS),
            "KY" => Some(KY), "LA" => Some(LA), "MA" => Some(MA), "MD" => Some(MD), "ME" => Some(ME),
            "MH" => Some(MH), "MI" => Some(MI), "MN" => Some(MN), "MO" => Some(MO), "MP" => Some(MP),
            "MS" => Some(MS), "MT" => Some(MT), "NC" => Some(NC), "ND" => Some(ND), "NE" => Some(NE),
            "NH" => Some(NH), "NJ" => Some(NJ), "NM" => Some(NM), "NV" => Some(NV), "NY" => Some(NY),
            "OH" => Some(OH), "OK" => Some(OK), "OR" => Some(OR), "PA" => Some(PA), "PR" => Some(PR),
            "PW" => Some(PW), "RI" => Some(RI), "SC" => Some(SC), "SD" => Some(SD), "TN" => Some(TN),
            "TX" => Some(TX), "UT" => Some(UT), "VA" => Some(VA), "VI" => Some(VI), "VT" => Some(VT),
            "WA" => Some(WA), "WI" => Some(WI), "WV" => Some(WV), "WY" => Some(WY), "ZZ" => Some(ZZ),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        use StateCode::*;
        match self {
            AK => "AK", AL => "AL", AR => "AR", AS => "AS", AZ => "AZ", CA => "CA", CO => "CO", CT => "CT", DC => "DC", DE => "DE", FL => "FL", FM => "FM", GA => "GA", GU => "GU", HI => "HI", IA => "IA", ID => "ID", IL => "IL", IN => "IN", KS => "KS", KY => "KY", LA => "LA", MA => "MA", MD => "MD", ME => "ME", MH => "MH", MI => "MI", MN => "MN", MO => "MO", MP => "MP", MS => "MS", MT => "MT", NC => "NC", ND => "ND", NE => "NE", NH => "NH", NJ => "NJ", NM => "NM", NV => "NV", NY => "NY", OH => "OH", OK => "OK", OR => "OR", PA => "PA", PR => "PR", PW => "PW", RI => "RI", SC => "SC", SD => "SD", TN => "TN", TX => "TX", UT => "UT", VA => "VA", VI => "VI", VT => "VT", WA => "WA", WI => "WI", WV => "WV", WY => "WY", ZZ => "ZZ",
        }
    }
}

/// Country Code (ISO 3166-1 alpha-2, plus US, ZZ, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountryCode(pub String);

impl CountryCode {
    pub fn from_code(code: &str) -> Self {
        CountryCode(code.to_ascii_uppercase())
    }
    pub fn as_code(&self) -> &str {
        &self.0
    }
}

/// Other Provider Identifier Issuer Code (01, 05, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OtherProviderIdentifierIssuerCode {
    Other,    // 01
    Medicaid, // 05
    // Extend as needed
}

impl OtherProviderIdentifierIssuerCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "01" => Some(OtherProviderIdentifierIssuerCode::Other),
            "05" => Some(OtherProviderIdentifierIssuerCode::Medicaid),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            OtherProviderIdentifierIssuerCode::Other => "01",
            OtherProviderIdentifierIssuerCode::Medicaid => "05",
        }
    }
}

/// Primary Taxonomy Switch (X, Y, N)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrimaryTaxonomySwitch {
    NotAnswered, // X
    Yes,         // Y
    No,          // N
}

impl PrimaryTaxonomySwitch {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "X" => Some(PrimaryTaxonomySwitch::NotAnswered),
            "Y" => Some(PrimaryTaxonomySwitch::Yes),
            "N" => Some(PrimaryTaxonomySwitch::No),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            PrimaryTaxonomySwitch::NotAnswered => "X",
            PrimaryTaxonomySwitch::Yes => "Y",
            PrimaryTaxonomySwitch::No => "N",
        }
    }
}

/// Group Taxonomy Code (193200000X, 193400000X)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupTaxonomyCode {
    MultiSpecialtyGroup,   // 193200000X
    SingleSpecialtyGroup,  // 193400000X
}

impl GroupTaxonomyCode {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "193200000X" => Some(GroupTaxonomyCode::MultiSpecialtyGroup),
            "193400000X" => Some(GroupTaxonomyCode::SingleSpecialtyGroup),
            _ => None,
        }
    }
    pub fn as_code(&self) -> &'static str {
        match self {
            GroupTaxonomyCode::MultiSpecialtyGroup => "193200000X",
            GroupTaxonomyCode::SingleSpecialtyGroup => "193400000X",
        }
    }
} 