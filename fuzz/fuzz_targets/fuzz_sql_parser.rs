#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(sql) = std::str::from_utf8(data) {
        // Parser should never panic on any input; errors are expected.
        let _ = blaze::sql::Parser::parse(sql);
    }
});
