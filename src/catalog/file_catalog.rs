use async_trait::async_trait;
use core::str;
use iceberg_rust::{
    catalog::{
        Catalog,
        commit::{CommitTable, CommitView, apply_table_updates, check_table_requirements},
        create::{CreateMaterializedView, CreateTable, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    object_store::{
        Bucket, ObjectStoreBuilder,
        store::{IcebergStore, version_hint_content},
    },
    spec::{
        identifier::FullIdentifier,
        table_metadata::{TableMetadata, new_metadata_location},
    },
    table::Table,
    view::View,
};
use object_store::{ObjectStore, PutOptions, UpdateVersion};
use std::{collections::HashMap, sync::Arc};

const VERSION_HINT_FILE_NAME: &str = "version-hint.text";
const METADATA_FILE_EXTENSION: &str = ".metadata.json";
const METADATA_PATH: &str = "/metadata/";

#[derive(Debug)]
pub struct FileCatalog {
    base_path: String,
    object_store_builder: ObjectStoreBuilder,
}

impl FileCatalog {
    pub fn new(
        base_path: &str,
        object_store_builder: ObjectStoreBuilder,
    ) -> Result<Self, iceberg_rust::error::Error> {
        Ok(FileCatalog {
            base_path: base_path.to_owned(),
            object_store_builder,
        })
    }

    fn get_object_store_and_path(&self) -> Result<(Arc<dyn ObjectStore>, String), IcebergError> {
        let bucket = Bucket::from_path(&self.base_path)?;
        let (_, path) = self
            .base_path
            .split_once(&bucket.to_string())
            .unwrap_or(("", ""));
        let object_store = self.object_store_builder.build(bucket)?;
        Ok((object_store, path.to_owned()))
    }

    fn identifier(&self) -> Identifier {
        Identifier::new(&Namespace::empty(), self.name())
    }

    fn identifier_location(&self, identifier: &Identifier) -> Result<String, IcebergError> {
        if !identifier.namespace().is_empty() {
            return Err(IcebergError::NotFound(format!(
                "namespace {}",
                identifier.namespace()
            )));
        }

        let (_, path) = self.get_object_store_and_path()?;
        Ok(path + "/" + identifier.name())
    }

    fn version_hint_location(&self, identifier: &Identifier) -> Result<String, IcebergError> {
        Ok(self.identifier_location(identifier)? + METADATA_PATH + VERSION_HINT_FILE_NAME)
    }

    async fn metadata_location(
        &self,
        identifier: &Identifier,
    ) -> Result<(String, UpdateVersion), IcebergError> {
        let (object_store, _) = self.get_object_store_and_path()?;
        let version_hint_location = self.version_hint_location(identifier)?;
        let version_hint_result = object_store.get(&version_hint_location.into()).await?;
        let tabular_location = self.identifier_location(identifier)?;
        let update_version = UpdateVersion {
            e_tag: version_hint_result.meta.e_tag.clone(),
            version: version_hint_result.meta.version.clone(),
        };
        let version_hint_content = version_hint_result.bytes().await?;
        let version = str::from_utf8(&version_hint_content)?;
        let metadata_location =
            tabular_location + METADATA_PATH + version + METADATA_FILE_EXTENSION;

        Ok((metadata_location, update_version))
    }

    async fn load_tabular_with_update_version(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<(Tabular, UpdateVersion), IcebergError> {
        let (metadata_location, version) = self.metadata_location(identifier).await?;
        let (object_store, _) = self.get_object_store_and_path()?;

        let metadata_content = object_store
            .get(&metadata_location.clone().into())
            .await?
            .bytes()
            .await?;

        let metadata: TableMetadata = serde_json::from_slice(&metadata_content)?;
        let table = Tabular::Table(
            Table::new(
                identifier.clone(),
                self.clone(),
                object_store.clone(),
                metadata,
            )
            .await?,
        );

        Ok((table, version))
    }

    async fn put_version_hint(
        &self,
        identifier: &Identifier,
        metadata_location: &str,
        opts: PutOptions,
    ) -> Result<(), IcebergError> {
        let (object_store, _) = self.get_object_store_and_path()?;
        let version_hint_location = self.version_hint_location(identifier)?;
        let content = version_hint_content(metadata_location);

        object_store
            .put_opts(&version_hint_location.into(), content.into(), opts)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Catalog for FileCatalog {
    fn name(&self) -> &str {
        self.base_path
            .trim_end_matches('/')
            .split("/")
            .last()
            .unwrap()
    }

    async fn create_namespace(
        &self,
        _namespace: &Namespace,
        _properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        unimplemented!()
    }

    async fn drop_namespace(&self, _namespace: &Namespace) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn load_namespace(
        &self,
        _namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        unimplemented!()
    }

    async fn update_namespace(
        &self,
        _namespace: &Namespace,
        _updates: Option<HashMap<String, String>>,
        _removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, IcebergError> {
        Ok(namespace.is_empty())
    }

    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        if !namespace.is_empty() {
            return Err(IcebergError::NotFound(format!("namespace {}", namespace)));
        }

        let (object_store, path) = self.get_object_store_and_path()?;

        Ok(object_store
            .list_with_delimiter(Some(&path.into()))
            .await
            .map_err(IcebergError::from)?
            .common_prefixes
            .into_iter()
            .map(|x| {
                Identifier::new(
                    &Namespace::empty(),
                    x.as_ref().trim_end_matches('/').split("/").last().unwrap(),
                )
            })
            .collect())
    }

    async fn list_namespaces(&self, _: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        Ok(vec![])
    }

    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        let (object_store, _) = self.get_object_store_and_path()?;
        let location = self.version_hint_location(identifier)?;

        match object_store.head(&location.into()).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn drop_table(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn drop_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn drop_materialized_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let (table, _) = self.load_tabular_with_update_version(identifier).await?;
        Ok(table)
    }

    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        mut create_table: CreateTable,
    ) -> Result<Table, IcebergError> {
        if self.tabular_exists(&identifier).await? {
            return Err(IcebergError::InvalidFormat(
                "Table already exists. Path".to_owned(),
            ));
        }

        create_table.location = Some(self.base_path.clone() + "/" + identifier.name());
        let (object_store, _) = self.get_object_store_and_path()?;

        let metadata: TableMetadata = create_table.try_into()?;
        let metadata_location = new_metadata_location(&metadata);

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        self.put_version_hint(
            &identifier,
            &metadata_location,
            PutOptions {
                mode: object_store::PutMode::Create,
                tags: object_store::TagSet::default(),
                attributes: object_store::Attributes::default(),
                extensions: object_store::Extensions::default(),
            },
        )
        .await?;

        Ok(Table::new(
            self.identifier(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn create_view(
        self: Arc<Self>,
        _identifier: Identifier,
        mut _create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        unimplemented!()
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        _identifier: Identifier,
        _create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, IcebergError> {
        unimplemented!()
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let (object_store, _path) = self.get_object_store_and_path()?;
        let (tabular, version) = self
            .clone()
            .load_tabular_with_update_version(&commit.identifier)
            .await?;

        let Tabular::Table(table) = tabular else {
            return Err(IcebergError::NotSupported(
                "cannot update a Tabular that is not a Table".to_owned(),
            ));
        };

        let mut metadata = table.metadata().clone();

        if !check_table_requirements(&commit.requirements, &metadata) {
            return Err(IcebergError::InvalidFormat(
                "Table requirements not valid".to_owned(),
            ));
        }

        apply_table_updates(&mut metadata, commit.updates)?;

        let metadata_location = new_metadata_location(&metadata);

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        self.put_version_hint(
            &commit.identifier,
            &metadata_location,
            PutOptions {
                mode: object_store::PutMode::Update(version),
                tags: object_store::TagSet::default(),
                attributes: object_store::Attributes::default(),
                extensions: object_store::Extensions::default(),
            },
        )
        .await?;

        Ok(Table::new(
            commit.identifier.clone(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn update_view(
        self: Arc<Self>,
        _commit: CommitView<Option<()>>,
    ) -> Result<View, IcebergError> {
        unimplemented!()
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        _commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, IcebergError> {
        unimplemented!()
    }

    async fn register_table(
        self: Arc<Self>,
        _identifier: Identifier,
        _metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        unimplemented!()
    }
}
