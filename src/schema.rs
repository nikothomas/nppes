/*!
 * Schema definitions for NPPES data files
 * 
 * This module contains the exact column mappings and schema definitions
 * for all NPPES data files as specified in the official documentation.
 */


/// Main NPPES data file schema
/// 
/// Defines the 330+ columns in the main npidata_pfile CSV file
pub struct NppesMainSchema;

impl NppesMainSchema {
    /// Get all column names in the exact order they appear in the CSV file
    pub fn column_names() -> Vec<&'static str> {
        let mut columns = vec![
            // Core identifiers
            "NPI",
            "Entity Type Code", 
            "Replacement NPI",
            "Employer Identification Number (EIN)",
            
            // Organization name
            "Provider Organization Name (Legal Business Name)",
            
            // Individual provider name
            "Provider Last Name (Legal Name)",
            "Provider First Name",
            "Provider Middle Name", 
            "Provider Name Prefix Text",
            "Provider Name Suffix Text",
            "Provider Credential Text",
            
            // Other organization name
            "Provider Other Organization Name",
            "Provider Other Organization Name Type Code",
            
            // Other individual name
            "Provider Other Last Name",
            "Provider Other First Name", 
            "Provider Other Middle Name",
            "Provider Other Name Prefix Text",
            "Provider Other Name Suffix Text",
            "Provider Other Credential Text",
            "Provider Other Last Name Type Code",
            
            // Mailing address
            "Provider First Line Business Mailing Address",
            "Provider Second Line Business Mailing Address", 
            "Provider Business Mailing Address City Name",
            "Provider Business Mailing Address State Name",
            "Provider Business Mailing Address Postal Code",
            "Provider Business Mailing Address Country Code (If outside U.S.)",
            "Provider Business Mailing Address Telephone Number",
            "Provider Business Mailing Address Fax Number",
            
            // Practice location address
            "Provider First Line Business Practice Location Address",
            "Provider Second Line Business Practice Location Address",
            "Provider Business Practice Location Address City Name", 
            "Provider Business Practice Location Address State Name",
            "Provider Business Practice Location Address Postal Code",
            "Provider Business Practice Location Address Country Code (If outside U.S.)",
            "Provider Business Practice Location Address Telephone Number",
            "Provider Business Practice Location Address Fax Number",
            
            // Dates
            "Provider Enumeration Date",
            "Last Update Date",
            "NPI Deactivation Reason Code",
            "NPI Deactivation Date",
            "NPI Reactivation Date",
            
            // Provider gender - fixed column name
            "Provider Sex Code",
            
            // Authorized official
            "Authorized Official Last Name",
            "Authorized Official First Name", 
            "Authorized Official Middle Name",
            "Authorized Official Title or Position",
            "Authorized Official Telephone Number",
        ];
        
        // Add taxonomy columns
        columns.extend(Self::taxonomy_columns());
        
        // Add other identifier columns  
        columns.extend(Self::other_identifier_columns());
        
        // Add organization flags
        columns.extend([
            "Is Sole Proprietor",
            "Is Organization Subpart", 
            "Parent Organization LBN",
            "Parent Organization TIN",
            
            // Authorized official additional fields
            "Authorized Official Name Prefix Text",
            "Authorized Official Name Suffix Text",
            "Authorized Official Credential Text",
        ]);
        
        // Add taxonomy group columns
        columns.extend(Self::taxonomy_group_columns());
        
        // Add certification date
        columns.push("Certification Date");
        
        columns
    }
    
    /// Generate taxonomy-related column names (15 sets of 4 columns each)
    fn taxonomy_columns() -> Vec<&'static str> {
        vec![
            "Healthcare Provider Taxonomy Code_1",
            "Provider License Number_1",
            "Provider License Number State Code_1", 
            "Healthcare Provider Primary Taxonomy Switch_1",
            "Healthcare Provider Taxonomy Code_2",
            "Provider License Number_2",
            "Provider License Number State Code_2",
            "Healthcare Provider Primary Taxonomy Switch_2",
            "Healthcare Provider Taxonomy Code_3",
            "Provider License Number_3", 
            "Provider License Number State Code_3",
            "Healthcare Provider Primary Taxonomy Switch_3",
            "Healthcare Provider Taxonomy Code_4",
            "Provider License Number_4",
            "Provider License Number State Code_4",
            "Healthcare Provider Primary Taxonomy Switch_4",
            "Healthcare Provider Taxonomy Code_5",
            "Provider License Number_5",
            "Provider License Number State Code_5",
            "Healthcare Provider Primary Taxonomy Switch_5",
            "Healthcare Provider Taxonomy Code_6",
            "Provider License Number_6",
            "Provider License Number State Code_6",
            "Healthcare Provider Primary Taxonomy Switch_6",
            "Healthcare Provider Taxonomy Code_7", 
            "Provider License Number_7",
            "Provider License Number State Code_7",
            "Healthcare Provider Primary Taxonomy Switch_7",
            "Healthcare Provider Taxonomy Code_8",
            "Provider License Number_8",
            "Provider License Number State Code_8",
            "Healthcare Provider Primary Taxonomy Switch_8",
            "Healthcare Provider Taxonomy Code_9",
            "Provider License Number_9",
            "Provider License Number State Code_9",
            "Healthcare Provider Primary Taxonomy Switch_9",
            "Healthcare Provider Taxonomy Code_10",
            "Provider License Number_10",
            "Provider License Number State Code_10",
            "Healthcare Provider Primary Taxonomy Switch_10",
            "Healthcare Provider Taxonomy Code_11",
            "Provider License Number_11", 
            "Provider License Number State Code_11",
            "Healthcare Provider Primary Taxonomy Switch_11",
            "Healthcare Provider Taxonomy Code_12",
            "Provider License Number_12",
            "Provider License Number State Code_12",
            "Healthcare Provider Primary Taxonomy Switch_12",
            "Healthcare Provider Taxonomy Code_13",
            "Provider License Number_13",
            "Provider License Number State Code_13",
            "Healthcare Provider Primary Taxonomy Switch_13",
            "Healthcare Provider Taxonomy Code_14",
            "Provider License Number_14",
            "Provider License Number State Code_14",
            "Healthcare Provider Primary Taxonomy Switch_14",
            "Healthcare Provider Taxonomy Code_15",
            "Provider License Number_15",
            "Provider License Number State Code_15",
            "Healthcare Provider Primary Taxonomy Switch_15",
        ]
    }
    
    /// Generate other identifier column names (50 sets of 4 columns each)
    fn other_identifier_columns() -> Vec<&'static str> {
        // Generate all 50 sets programmatically
        vec![
            "Other Provider Identifier_1",
            "Other Provider Identifier Type Code_1",
            "Other Provider Identifier State_1", 
            "Other Provider Identifier Issuer_1",
            "Other Provider Identifier_2",
            "Other Provider Identifier Type Code_2",
            "Other Provider Identifier State_2",
            "Other Provider Identifier Issuer_2",
            "Other Provider Identifier_3",
            "Other Provider Identifier Type Code_3",
            "Other Provider Identifier State_3",
            "Other Provider Identifier Issuer_3",
            "Other Provider Identifier_4",
            "Other Provider Identifier Type Code_4", 
            "Other Provider Identifier State_4",
            "Other Provider Identifier Issuer_4",
            "Other Provider Identifier_5",
            "Other Provider Identifier Type Code_5",
            "Other Provider Identifier State_5",
            "Other Provider Identifier Issuer_5",
            "Other Provider Identifier_6",
            "Other Provider Identifier Type Code_6",
            "Other Provider Identifier State_6",
            "Other Provider Identifier Issuer_6",
            "Other Provider Identifier_7",
            "Other Provider Identifier Type Code_7",
            "Other Provider Identifier State_7",
            "Other Provider Identifier Issuer_7",
            "Other Provider Identifier_8",
            "Other Provider Identifier Type Code_8",
            "Other Provider Identifier State_8",
            "Other Provider Identifier Issuer_8",
            "Other Provider Identifier_9",
            "Other Provider Identifier Type Code_9",
            "Other Provider Identifier State_9",
            "Other Provider Identifier Issuer_9",
            "Other Provider Identifier_10",
            "Other Provider Identifier Type Code_10",
            "Other Provider Identifier State_10",
            "Other Provider Identifier Issuer_10",
            "Other Provider Identifier_11",
            "Other Provider Identifier Type Code_11",
            "Other Provider Identifier State_11",
            "Other Provider Identifier Issuer_11",
            "Other Provider Identifier_12",
            "Other Provider Identifier Type Code_12",
            "Other Provider Identifier State_12",
            "Other Provider Identifier Issuer_12",
            "Other Provider Identifier_13",
            "Other Provider Identifier Type Code_13",
            "Other Provider Identifier State_13",
            "Other Provider Identifier Issuer_13",
            "Other Provider Identifier_14",
            "Other Provider Identifier Type Code_14",
            "Other Provider Identifier State_14",
            "Other Provider Identifier Issuer_14",
            "Other Provider Identifier_15",
            "Other Provider Identifier Type Code_15",
            "Other Provider Identifier State_15",
            "Other Provider Identifier Issuer_15",
            "Other Provider Identifier_16",
            "Other Provider Identifier Type Code_16",
            "Other Provider Identifier State_16",
            "Other Provider Identifier Issuer_16",
            "Other Provider Identifier_17",
            "Other Provider Identifier Type Code_17",
            "Other Provider Identifier State_17",
            "Other Provider Identifier Issuer_17",
            "Other Provider Identifier_18",
            "Other Provider Identifier Type Code_18",
            "Other Provider Identifier State_18",
            "Other Provider Identifier Issuer_18",
            "Other Provider Identifier_19",
            "Other Provider Identifier Type Code_19",
            "Other Provider Identifier State_19",
            "Other Provider Identifier Issuer_19",
            "Other Provider Identifier_20",
            "Other Provider Identifier Type Code_20",
            "Other Provider Identifier State_20",
            "Other Provider Identifier Issuer_20",
            "Other Provider Identifier_21",
            "Other Provider Identifier Type Code_21",
            "Other Provider Identifier State_21",
            "Other Provider Identifier Issuer_21",
            "Other Provider Identifier_22",
            "Other Provider Identifier Type Code_22",
            "Other Provider Identifier State_22",
            "Other Provider Identifier Issuer_22",
            "Other Provider Identifier_23",
            "Other Provider Identifier Type Code_23",
            "Other Provider Identifier State_23",
            "Other Provider Identifier Issuer_23",
            "Other Provider Identifier_24",
            "Other Provider Identifier Type Code_24",
            "Other Provider Identifier State_24",
            "Other Provider Identifier Issuer_24",
            "Other Provider Identifier_25",
            "Other Provider Identifier Type Code_25",
            "Other Provider Identifier State_25",
            "Other Provider Identifier Issuer_25",
            "Other Provider Identifier_26",
            "Other Provider Identifier Type Code_26",
            "Other Provider Identifier State_26",
            "Other Provider Identifier Issuer_26",
            "Other Provider Identifier_27",
            "Other Provider Identifier Type Code_27",
            "Other Provider Identifier State_27",
            "Other Provider Identifier Issuer_27",
            "Other Provider Identifier_28",
            "Other Provider Identifier Type Code_28",
            "Other Provider Identifier State_28",
            "Other Provider Identifier Issuer_28",
            "Other Provider Identifier_29",
            "Other Provider Identifier Type Code_29",
            "Other Provider Identifier State_29",
            "Other Provider Identifier Issuer_29",
            "Other Provider Identifier_30",
            "Other Provider Identifier Type Code_30",
            "Other Provider Identifier State_30",
            "Other Provider Identifier Issuer_30",
            "Other Provider Identifier_31",
            "Other Provider Identifier Type Code_31",
            "Other Provider Identifier State_31",
            "Other Provider Identifier Issuer_31",
            "Other Provider Identifier_32",
            "Other Provider Identifier Type Code_32",
            "Other Provider Identifier State_32",
            "Other Provider Identifier Issuer_32",
            "Other Provider Identifier_33",
            "Other Provider Identifier Type Code_33",
            "Other Provider Identifier State_33",
            "Other Provider Identifier Issuer_33",
            "Other Provider Identifier_34",
            "Other Provider Identifier Type Code_34",
            "Other Provider Identifier State_34",
            "Other Provider Identifier Issuer_34",
            "Other Provider Identifier_35",
            "Other Provider Identifier Type Code_35",
            "Other Provider Identifier State_35",
            "Other Provider Identifier Issuer_35",
            "Other Provider Identifier_36",
            "Other Provider Identifier Type Code_36",
            "Other Provider Identifier State_36",
            "Other Provider Identifier Issuer_36",
            "Other Provider Identifier_37",
            "Other Provider Identifier Type Code_37",
            "Other Provider Identifier State_37",
            "Other Provider Identifier Issuer_37",
            "Other Provider Identifier_38",
            "Other Provider Identifier Type Code_38",
            "Other Provider Identifier State_38",
            "Other Provider Identifier Issuer_38",
            "Other Provider Identifier_39",
            "Other Provider Identifier Type Code_39",
            "Other Provider Identifier State_39",
            "Other Provider Identifier Issuer_39",
            "Other Provider Identifier_40",
            "Other Provider Identifier Type Code_40",
            "Other Provider Identifier State_40",
            "Other Provider Identifier Issuer_40",
            "Other Provider Identifier_41",
            "Other Provider Identifier Type Code_41",
            "Other Provider Identifier State_41",
            "Other Provider Identifier Issuer_41",
            "Other Provider Identifier_42",
            "Other Provider Identifier Type Code_42",
            "Other Provider Identifier State_42",
            "Other Provider Identifier Issuer_42",
            "Other Provider Identifier_43",
            "Other Provider Identifier Type Code_43",
            "Other Provider Identifier State_43",
            "Other Provider Identifier Issuer_43",
            "Other Provider Identifier_44",
            "Other Provider Identifier Type Code_44",
            "Other Provider Identifier State_44",
            "Other Provider Identifier Issuer_44",
            "Other Provider Identifier_45",
            "Other Provider Identifier Type Code_45",
            "Other Provider Identifier State_45",
            "Other Provider Identifier Issuer_45",
            "Other Provider Identifier_46",
            "Other Provider Identifier Type Code_46",
            "Other Provider Identifier State_46",
            "Other Provider Identifier Issuer_46",
            "Other Provider Identifier_47",
            "Other Provider Identifier Type Code_47",
            "Other Provider Identifier State_47",
            "Other Provider Identifier Issuer_47",
            "Other Provider Identifier_48",
            "Other Provider Identifier Type Code_48",
            "Other Provider Identifier State_48",
            "Other Provider Identifier Issuer_48",
            "Other Provider Identifier_49",
            "Other Provider Identifier Type Code_49",
            "Other Provider Identifier State_49",
            "Other Provider Identifier Issuer_49",
            "Other Provider Identifier_50",
            "Other Provider Identifier Type Code_50",
            "Other Provider Identifier State_50",
            "Other Provider Identifier Issuer_50",
        ]
    }
    
    /// Generate taxonomy group column names (15 groups)
    fn taxonomy_group_columns() -> Vec<&'static str> {
        vec![
            "Healthcare Provider Taxonomy Group_1",
            "Healthcare Provider Taxonomy Group_2", 
            "Healthcare Provider Taxonomy Group_3",
            "Healthcare Provider Taxonomy Group_4",
            "Healthcare Provider Taxonomy Group_5",
            "Healthcare Provider Taxonomy Group_6",
            "Healthcare Provider Taxonomy Group_7",
            "Healthcare Provider Taxonomy Group_8",
            "Healthcare Provider Taxonomy Group_9",
            "Healthcare Provider Taxonomy Group_10",
            "Healthcare Provider Taxonomy Group_11",
            "Healthcare Provider Taxonomy Group_12",
            "Healthcare Provider Taxonomy Group_13",
            "Healthcare Provider Taxonomy Group_14", 
            "Healthcare Provider Taxonomy Group_15",
        ]
    }
    
    /// Get the total number of columns in the main NPPES file
    pub fn column_count() -> usize {
        Self::column_names().len()
    }
    
    /// Validate that a header row matches the expected NPPES schema
    pub fn validate_headers(headers: &[String]) -> Result<(), crate::NppesError> {
        let expected_columns = Self::column_names();
        
        if headers.len() != expected_columns.len() {
            return Err(crate::NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                return Err(crate::NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        Ok(())
    }
}

/// Other Name Reference file schema
pub struct OtherNameSchema;

impl OtherNameSchema {
    pub fn column_names() -> Vec<&'static str> {
        vec![
            "NPI",
            "Provider Other Organization Name",
            "Provider Other Organization Name Type Code",
        ]
    }
    
    pub fn column_count() -> usize {
        3
    }
    
    pub fn validate_headers(headers: &[String]) -> Result<(), crate::NppesError> {
        let expected_columns = Self::column_names();
        
        if headers.len() != expected_columns.len() {
            return Err(crate::NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                return Err(crate::NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        Ok(())
    }
}

/// Practice Location Reference file schema
pub struct PracticeLocationSchema;

impl PracticeLocationSchema {
    pub fn column_names() -> Vec<&'static str> {
        vec![
            "NPI",
            "Provider Secondary Practice Location Address- Address Line 1",
            "Provider Secondary Practice Location Address-  Address Line 2",
            "Provider Secondary Practice Location Address - City Name",
            "Provider Secondary Practice Location Address - State Name", 
            "Provider Secondary Practice Location Address - Postal Code",
            "Provider Secondary Practice Location Address - Country Code (If outside U.S.)",
            "Provider Secondary Practice Location Address - Telephone Number",
            "Provider Secondary Practice Location Address - Telephone Extension",
            "Provider Practice Location Address - Fax Number",
        ]
    }
    
    pub fn column_count() -> usize {
        10
    }
    
    pub fn validate_headers(headers: &[String]) -> Result<(), crate::NppesError> {
        let expected_columns = Self::column_names();
        
        if headers.len() != expected_columns.len() {
            return Err(crate::NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                return Err(crate::NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        Ok(())
    }
}

/// Endpoint Reference file schema
pub struct EndpointSchema;

impl EndpointSchema {
    pub fn column_names() -> Vec<&'static str> {
        vec![
            "NPI",
            "Endpoint Type",
            "Endpoint Type Description", 
            "Endpoint",
            "Affiliation",
            "Endpoint Description",
            "Affiliation Legal Business Name",
            "Use Code",
            "Use Description",
            "Other Use Description",
            "Content Type",
            "Content Description",
            "Other Content Description",
            "Affiliation Address Line One",
            "Affiliation Address Line Two",
            "Affiliation Address City",
            "Affiliation Address State", 
            "Affiliation Address Country",
            "Affiliation Address Postal Code",
        ]
    }
    
    pub fn column_count() -> usize {
        19
    }
    
    pub fn validate_headers(headers: &[String]) -> Result<(), crate::NppesError> {
        let expected_columns = Self::column_names();
        
        if headers.len() != expected_columns.len() {
            return Err(crate::NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                return Err(crate::NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        Ok(())
    }
}

/// Healthcare taxonomy reference schema
pub struct TaxonomySchema;

impl TaxonomySchema {
    pub fn column_names() -> Vec<&'static str> {
        vec![
            "Code",
            "Grouping",
            "Classification", 
            "Specialization",
            "Definition",
            "Notes",
            "Display Name",
            "Section",
        ]
    }
    
    pub fn column_count() -> usize {
        8
    }
    
    pub fn validate_headers(headers: &[String]) -> Result<(), crate::NppesError> {
        let expected_columns = Self::column_names();
        
        if headers.len() != expected_columns.len() {
            return Err(crate::NppesError::schema_mismatch_detailed(
                expected_columns.len(),
                headers.len(),
                None,
            ));
        }
        
        for (i, (expected, actual)) in expected_columns.iter().zip(headers.iter()).enumerate() {
            if expected != actual {
                return Err(crate::NppesError::schema_mismatch_detailed(
                    expected_columns.len(),
                    headers.len(),
                    Some((i, expected.to_string(), actual.clone())),
                ));
            }
        }
        
        Ok(())
    }
} 