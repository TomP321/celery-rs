//! Sets libpython's runtime search path for cargo test; only maturin builds enable extension-module.
fn main() {
    pyo3_build_config::add_libpython_rpath_link_args();
}
