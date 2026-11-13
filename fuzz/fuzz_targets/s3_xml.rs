#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = s3_sidecar::fuzz_support::s3_xml(data);
});
