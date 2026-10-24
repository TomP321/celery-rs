//! Rpath verso libpython per i binari di test.
//!
//! La feature `extension-module` è attivata solo dai build di maturin: `cargo test`
//! linka libpython e, senza rpath, il binario di test non la trova a runtime.
fn main() {
    pyo3_build_config::add_libpython_rpath_link_args();
}
