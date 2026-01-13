use super::FileCatalog;
use super::error::Error;
use crate::object_store::parse_url_opts;
use iceberg_rest_catalog::apis::configuration::ConfigurationBuilder;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_rust::table::Table;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub async fn load_catalog_and_table<I>(
    props: I,
    table: impl AsRef<str>,
) -> Result<(Arc<dyn Catalog>, Table), Error>
where
    I: IntoIterator<Item = (String, String)>,
{
    let catalog = load_catalog(props).await?;

    let Tabular::Table(table) = catalog
        .clone()
        .load_tabular(&Identifier::new(&Namespace::empty(), table.as_ref()))
        .await?
    else {
        return Err(Error::NotATable(table.as_ref().to_owned()));
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

            Ok(Arc::new(RestCatalog::new(
                m.get("name").map(|x| x.as_str()),
                b.build().map_err(|e| Error::RestConfigurationbuilder(e))?,
                os.and_then(|x| Some(x.1)),
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
