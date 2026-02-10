//! Table name validation for SQL injection prevention
//!
//! This module provides the [`ValidatedTableName`] newtype which ensures
//! table names are validated against an allowlist before use in SQL queries.

use crate::error::{Result, SyncError};
use std::collections::HashSet;

/// A table name that has been validated against the database schema.
///
/// This type can only be constructed via [`ValidatedTableName::new`] or
/// [`TableSchema::validate`], ensuring all table names used in SQL queries
/// have been verified to exist in the database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatedTableName(String);

impl ValidatedTableName {
    /// Create a new validated table name.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::InvalidTableName`] if the name is not in the allowlist.
    pub fn new(name: &str, allowed_tables: &HashSet<String>) -> Result<Self> {
        if allowed_tables.contains(name) {
            Ok(Self(name.to_string()))
        } else {
            let mut available: Vec<_> = allowed_tables.iter().cloned().collect();
            available.sort();
            Err(SyncError::InvalidTableName {
                name: name.to_string(),
                available: available.join(", "),
            })
        }
    }

    /// Get the table name as a string slice.
    #[allow(dead_code)]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the validated name and return the inner string.
    #[allow(dead_code)]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for ValidatedTableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ValidatedTableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Schema information used for table name validation.
///
/// Holds the set of valid table names queried from the database.
#[derive(Debug, Clone)]
pub struct TableSchema {
    tables: HashSet<String>,
}

impl TableSchema {
    /// Create a new schema from an iterator of table names.
    pub fn new(tables: impl IntoIterator<Item = String>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }

    /// Validate a table name against this schema.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::InvalidTableName`] if the table doesn't exist.
    pub fn validate(&self, name: &str) -> Result<ValidatedTableName> {
        ValidatedTableName::new(name, &self.tables)
    }

    /// Check if a table name exists in the schema.
    #[allow(dead_code)]
    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains(name)
    }

    /// Get the number of tables in the schema.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Check if the schema is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get an iterator over all table names.
    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.tables.iter()
    }

    /// Get the underlying set of table names (for ValidatedTableName::new).
    #[allow(dead_code)]
    pub fn tables(&self) -> &HashSet<String> {
        &self.tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> TableSchema {
        TableSchema::new(vec![
            "users".to_string(),
            "posts".to_string(),
            "comments".to_string(),
        ])
    }

    #[test]
    fn validate_existing_table() {
        let schema = test_schema();
        let validated = schema.validate("users").unwrap();
        assert_eq!(validated.as_str(), "users");
    }

    #[test]
    fn reject_nonexistent_table() {
        let schema = test_schema();
        let result = schema.validate("nonexistent");
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            SyncError::InvalidTableName { name, available } => {
                assert_eq!(name, "nonexistent");
                assert!(available.contains("users"));
                assert!(available.contains("posts"));
                assert!(available.contains("comments"));
            }
            _ => panic!("Expected InvalidTableName error"),
        }
    }

    #[test]
    fn reject_sql_injection_attempts() {
        let schema = test_schema();

        let injection_attempts = vec![
            "users; DROP TABLE users;--",
            "users' OR '1'='1",
            "users\" OR \"1\"=\"1",
            "users/**/UNION/**/SELECT/**/password/**/FROM/**/admin",
            "users; DELETE FROM users WHERE 1=1;--",
            "../../../etc/passwd",
        ];

        for attempt in injection_attempts {
            let result = schema.validate(attempt);
            assert!(
                result.is_err(),
                "Should reject injection attempt: {}",
                attempt
            );
        }
    }

    #[test]
    fn schema_contains() {
        let schema = test_schema();
        assert!(schema.contains("users"));
        assert!(schema.contains("posts"));
        assert!(!schema.contains("nonexistent"));
    }

    #[test]
    fn schema_len() {
        let schema = test_schema();
        assert_eq!(schema.len(), 3);
        assert!(!schema.is_empty());
    }

    #[test]
    fn empty_schema() {
        let schema = TableSchema::new(Vec::<String>::new());
        assert!(schema.is_empty());
        assert_eq!(schema.len(), 0);
        assert!(schema.validate("anything").is_err());
    }

    #[test]
    fn validated_name_display() {
        let schema = test_schema();
        let validated = schema.validate("users").unwrap();
        assert_eq!(format!("{}", validated), "users");
    }

    #[test]
    fn validated_name_into_string() {
        let schema = test_schema();
        let validated = schema.validate("users").unwrap();
        let s: String = validated.into_string();
        assert_eq!(s, "users");
    }

    #[test]
    fn validated_name_as_ref() {
        let schema = test_schema();
        let validated = schema.validate("users").unwrap();
        let s: &str = validated.as_ref();
        assert_eq!(s, "users");
    }

    #[test]
    fn table_names_are_case_sensitive() {
        let schema = test_schema();
        // SQLite table names are case-sensitive by default
        assert!(schema.validate("USERS").is_err());
        assert!(schema.validate("Users").is_err());
        assert!(schema.validate("users").is_ok());
    }
}
