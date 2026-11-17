fn main() {
    if std::env::var_os("CARGO_FEATURE_EMBED_UI").is_some() {
        let dist = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../ui/dist");
        assert!(
            dist.join("index.html").is_file(),
            "embed-ui requires ui/dist/index.html; run npm ci --prefix ui && npm run build --prefix ui first"
        );
        println!("cargo:rerun-if-changed={}", dist.display());
    }
}
