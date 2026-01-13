use iceberg_rust::object_store::ObjectStoreBuilder;
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    path::Path,
    ObjectStoreScheme,
};
use querystring::querify;
use std::str::FromStr;
use url::Url;

pub fn opts_from_query_string(s: &str) -> Vec<(String, String)> {
    querify(s)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub fn opts_from_env() -> Vec<(String, String)> {
    std::env::vars()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect()
}

pub fn parse_url_opts<I, K, V>(
    u: &Url,
    opts: I,
) -> Result<(ObjectStoreBuilder, Path), object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (scheme, path) = ObjectStoreScheme::parse(u)?;
    let path = Path::parse(path)?;

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let mut b = AmazonS3Builder::new();
            b = b.with_url(u.to_string());

            for (k, v) in opts {
                if let Ok(k) = AmazonS3ConfigKey::from_str(k.as_ref()) {
                    b = b.with_config(k, v);
                }
            }

            Ok((ObjectStoreBuilder::S3(Box::new(b)), path))
        }
        _ => Err(object_store::Error::NotImplemented),
    }
}
