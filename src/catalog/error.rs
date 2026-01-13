use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    URLParse(#[from] url::ParseError),
    #[error(transparent)]
    Iceberg(#[from] iceberg_rust::error::Error),
    #[error(transparent)]
    RestConfigurationbuilder(
        #[from] iceberg_rest_catalog::apis::configuration::ConfigurationBuilderError,
    ),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("Missing property: `{0}`")]
    MissingProperty(String),
    #[error("Bad property value for `{0}`: {1}")]
    BadPropertyValue(String, String),
    #[error("Not a table: `{0}`")]
    NotATable(String),
}
