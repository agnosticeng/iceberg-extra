use url::Url;

pub trait UrlExt {
    fn cleanup_empty_path_segments(&mut self);
}

impl UrlExt for Url {
    fn cleanup_empty_path_segments(&mut self) {
        let clean_path = self
            .path()
            .split('/')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("/");

        self.set_path(&clean_path);
    }
}

pub fn cleanup_url_empty_path_segments(s: &str) -> Result<String, url::ParseError> {
    let mut u = Url::parse(s)?;
    u.cleanup_empty_path_segments();
    Ok(u.to_string())
}
