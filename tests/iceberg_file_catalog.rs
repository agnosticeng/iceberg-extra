extern crate iceberg_extra;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::record_batch;
use futures::stream::{self};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{
        Catalog,
        create::CreateTableBuilder,
        tabular::Tabular
    },
    spec::{
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
        identifier::Identifier,
        namespace::Namespace
    }
};
use iceberg_extra::{
    catalog::FileCatalog,
    object_store::parse_url_opts
};
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

fn new_file_catalog(base_path: &str) -> Result<Arc<FileCatalog>> {
    let u = Url::parse(&format!("s3://test01/{}", base_path.to_owned()))?;
    let (object_store_builder, _) = parse_url_opts(
        &u,
        vec![
            ("aws_endpoint", "http://localhost:9001"),
            ("aws_region", "us-east-1"),
            ("aws_access_key_id", "minio"),
            ("aws_secret_access_key", "minio123"),
            ("aws_allow_http", "true"),
            ("aws_virtual_hosted_style_request", "false"),
        ],
    )?;
    Ok(Arc::new(FileCatalog::new(
        &u.to_string(),
        object_store_builder,
    )?))
}

fn new_schema() -> Result<Schema> {
    Ok(Schema::builder()
        .with_struct_field(StructField {
            id: 1,
            name: "key".to_owned(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
        })
        .with_struct_field(StructField {
            id: 2,
            name: "value".to_owned(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
        })
        .build()?)
}

#[tokio::test]
async fn test_iceberg_file_catalog() -> Result<()> {
    let base_path = Uuid::new_v4();
    let catalog = new_file_catalog(&base_path.to_string())?;
    assert_eq!(catalog.name(), base_path.to_string());

    CreateTableBuilder::default()
        .with_name("table01")
        .with_schema(new_schema()?)
        .build(&[], catalog.clone())
        .await?;

    let Tabular::Table(mut table) = catalog
        .clone()
        .load_tabular(&Identifier::new(&[], "table01"))
        .await?
    else {
        return Err(anyhow!("Tabular is not a table"));
    };

    assert_eq!(table.identifier().name(), "table01");

    assert_eq!(
        catalog
            .tabular_exists(&Identifier::new(&[], "table01"))
            .await?,
        true
    );

    assert_eq!(
        catalog
            .tabular_exists(&Identifier::new(&[], "table02"))
            .await?,
        false
    );

    assert!(
        CreateTableBuilder::default()
            .with_name("table01")
            .with_schema(new_schema()?)
            .build(&[], catalog.clone())
            .await
            .is_err()
    );

    CreateTableBuilder::default()
        .with_name("table02")
        .with_schema(new_schema()?)
        .build(&[], catalog.clone())
        .await?;

    let batches = vec![record_batch!(
        ("key", Utf8, ["a", "b", "c"]),
        ("value", Utf8, ["123", "456", "789"])
    )];

    let new_data_files =
        write_parquet_partitioned(&table, stream::iter(batches.into_iter()), None).await?;

    table
        .new_transaction(None)
        .append_data(new_data_files)
        .commit()
        .await?;

    let tables = catalog.list_tabulars(&Namespace::empty()).await?;

    assert_eq!(
        tables,
        [
            Identifier::new(&Namespace::empty(), "table01"),
            Identifier::new(&Namespace::empty(), "table02")
        ]
    );

    Ok(())
}
