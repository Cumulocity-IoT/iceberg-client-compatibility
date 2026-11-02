//! Connect Apache DataFusion to the Streaming Lake Ingestion lake and read a few rows.
//!
//! The catalog settings come from the environment (see the README). The lake is registered as the
//! DataFusion catalog `lake`, so a table is addressed as lake.<namespace>."<table>". DataFusion
//! lowercases unquoted identifiers, so quote every name that has upper-case letters.
//!
//! iceberg-catalog-rest has no TLS backend of its own, so Cargo.toml also needs
//! `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }` for an https
//! catalog (apache/iceberg-rust#2888); see the `tls` feature in this crate's Cargo.toml.

use std::collections::HashMap;
use std::env::var;
use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;

pub type Res<T> = Result<T, Box<dyn Error>>;

/// The tenant's Iceberg REST catalog, with storage credentials vended by the catalog.
pub async fn catalog() -> Res<Arc<RestCatalog>> {
    let mut props: HashMap<String, String> = HashMap::new();
    for (key, env) in [
        ("uri", "CATALOG_URI"),
        ("warehouse", "CATALOG_WAREHOUSE"),
        ("credential", "CATALOG_CREDENTIAL"), // <clientId>:<clientSecret>
        ("oauth2-server-uri", "CATALOG_TOKEN_URI"),
        ("scope", "CATALOG_SCOPE"), // PRINCIPAL_ROLE:ALL; iceberg-rust's default "catalog" is rejected
    ] {
        props.insert(key.into(), var(env).map_err(|_| format!("{env} is not set"))?);
    }
    props.insert("header.X-Iceberg-Access-Delegation".into(), "vended-credentials".into());

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 { customized_credential_load: None }))
        .load("lake", props)
        .await?;
    Ok(Arc::new(catalog))
}

/// A DataFusion session with the catalog registered as `lake`.
pub async fn session(catalog: Arc<RestCatalog>) -> Res<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_catalog("lake", Arc::new(IcebergCatalogProvider::try_new(catalog).await?));
    Ok(ctx)
}

pub async fn read_a_few_rows(ctx: &SessionContext) -> Res<Vec<RecordBatch>> {
    let sql = r#"SELECT source, "time", "T"."value" FROM lake.cdc_measurement."c8y_Temperature" LIMIT 5"#;
    Ok(ctx.sql(sql).await?.collect().await?)
}

