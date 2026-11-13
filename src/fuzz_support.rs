//! Stable entry points for the fuzz targets.
//!
//! The fuzz crate in `fuzz/` calls these functions. They accept arbitrary
//! input and do not panic. They are hidden from the rendered documentation.

/// Send arbitrary bytes to the `SigV4` parser and canonical request code.
///
/// The return value is `true` when the input parsed as an authorization
/// header. The fuzz target discards the value.
#[doc(hidden)]
#[must_use]
pub fn sigv4(input: &[u8]) -> bool {
    crate::auth::verify::fuzz_authorization(input)
}

/// Send arbitrary bytes to the S3 XML tag and list parsers.
///
/// The return value is a small count. The fuzz target discards the value.
#[doc(hidden)]
#[must_use]
pub fn s3_xml(input: &[u8]) -> usize {
    let text = String::from_utf8_lossy(input);
    crate::cluster::fuzz_s3_xml(&text).saturating_add(crate::cache::fuzz_s3_xml(&text))
}
