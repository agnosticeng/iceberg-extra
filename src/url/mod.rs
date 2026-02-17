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
