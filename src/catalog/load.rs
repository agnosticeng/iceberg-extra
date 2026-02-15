use super::FileCatalog;
use super::error::Error;
use crate::object_store::parse_url_opts;
use iceberg_rest_catalog::apis::configuration::ApiKey;
use iceberg_rest_catalog::apis::configuration::ConfigurationBuilder;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rest_catalog::configuration_rewriter::ConfigurationRewriter;
use iceberg_rest_catalog::oauth2_client_credentials::OAuth2ClientCredentials;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::spec::namespace::Namespace;
use iceberg_rust::table::Table;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub fn parse_identifier(s: &str) -> Result<Identifier, Error> {
    let parts = s.split(".").map(ToOwned::to_owned).collect::<Vec<String>>();

    match parts.len() {
        0 => Err(iceberg_rust::error::Error::InvalidFormat("identifier".to_owned()).into()),
        1 => Ok(Identifier::new(&Namespace::empty(), &parts[0])),
        n => Ok(Identifier::new(&parts[0..n - 1], &parts[n - 1])),
    }
}

pub async fn load_catalog_and_table<I>(
    props: I,
    table: &str,
) -> Result<(Arc<dyn Catalog>, Table), Error>
where
    I: IntoIterator<Item = (String, String)>,
{
    let catalog = load_catalog(props).await?;
    let id = parse_identifier(table)?;

    let Tabular::Table(table) = catalog.clone().load_tabular(&id).await? else {
        return Err(Error::NotATable(table.to_owned()));
    };

    Ok((catalog, table))
}

pub async fn load_catalog<I>(props: I) -> Result<Arc<dyn Catalog>, Error>
where
    I: IntoIterator<Item = (String, String)>,
{
    let m: HashMap<String, String> = props.into_iter().collect();

    match m.get("type") {
        Some(val) if val == "file" => {
            let (u, object_store_builder) = try_get_object_store_url_and_builder(&m)?
                .ok_or(Error::MissingProperty("storage_path".to_owned()))?;

            Ok(Arc::new(FileCatalog::new(
                u.as_ref(),
                object_store_builder,
            )?))
        }

        Some(val) if val == "rest" => {
            let os = try_get_object_store_url_and_builder(&m)?;
            let base_path = m
                .get("base_path")
                .ok_or(Error::MissingProperty("base_path".to_owned()))?;

            let mut b = &mut ConfigurationBuilder::default();
            b = b.base_path(base_path.to_owned());

            let rewriter = if let Some(s) = m.get("oauth_server_uri") {
                let rw: Arc<dyn ConfigurationRewriter> = Arc::new(OAuth2ClientCredentials::new(
                    s.to_owned(),
                    m.get("oauth_client_id")
                        .ok_or(Error::MissingProperty("oauth_client_id".to_owned()))?
                        .to_owned(),
                    m.get("oauth_client_secret")
                        .ok_or(Error::MissingProperty("oauth_client_secret".to_owned()))?
                        .to_owned(),
                    m.get("oauth_audience").cloned(),
                    m.get("oauth_scope").cloned(),
                ));

                Some(rw)
            } else {
                None
            };

            if let Some(s) = m.get("api_key") {
                b = b.api_key(ApiKey {
                    prefix: None,
                    key: s.to_owned(),
                });
            }

            if let Some(s) = m.get("oauth_access_token") {
                b = b.oauth_access_token(s);
            }

            if let Some(s) = m.get("bearer_access_token") {
                b = b.bearer_access_token(s);
            }

            Ok(Arc::new(RestCatalog::new(
                m.get("name").map(|x| x.as_str()),
                b.build().map_err(Error::RestConfigurationbuilder)?,
                rewriter,
                os.clone().map(|x| x.1),
                os.is_some(),
            )))
        }

        Some(val) => Err(Error::BadPropertyValue("type".to_owned(), val.to_owned())),
        None => Err(Error::MissingProperty("type".to_owned())),
    }
}

fn try_get_object_store_url_and_builder(
    m: &HashMap<String, String>,
) -> Result<Option<(Url, ObjectStoreBuilder)>, Error> {
    let Some(storage_path) = m.get("storage_path") else {
        return Ok(None);
    };
    let u = Url::parse(storage_path)?;
    let (object_store_builder, _) = parse_url_opts(&u, m)?;
    Ok(Some((u, object_store_builder)))
}
