use crate::error::{LogHavenError, Result};

const MAX_LEN: usize = 64;

const WINDOWS_RESERVED: [&str; 22] = [
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
    "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
];

/// App names become directory names, so anything that could escape or
/// confuse the data directory is rejected.
pub fn validate_app_name(app: &str) -> Result<()> {
    if app.is_empty() {
        return Err(LogHavenError::InvalidApp("app name is empty"));
    }
    if app.len() > MAX_LEN {
        return Err(LogHavenError::InvalidApp("app name longer than 64 bytes"));
    }
    if app.starts_with('.') || app.ends_with('.') {
        return Err(LogHavenError::InvalidApp(
            "app name must not start or end with '.'",
        ));
    }
    // lowercase only: "Web" and "web" are one directory on macOS/Windows
    if !app.bytes().all(|b| {
        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'_' || b == b'-'
    }) {
        return Err(LogHavenError::InvalidApp(
            "app name may only contain a-z 0-9 . _ -",
        ));
    }
    let stem = app.split('.').next().unwrap_or(app);
    if WINDOWS_RESERVED.contains(&stem) {
        return Err(LogHavenError::InvalidApp(
            "app name is a reserved device name",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_names() {
        for ok in [
            "web",
            "my-app",
            "my_app.v2",
            "a1",
            "console",
            &"a".repeat(64),
        ] {
            assert!(validate_app_name(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn rejects_traversal_and_separators() {
        for bad in [
            "..", ".", "../x", "x/../y", "a/b", "a\\b", "/abs", "C:\\x", ".hidden", "a\0b", "a b",
            "a\n", "index/..",
        ] {
            assert!(validate_app_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn rejects_empty_long_and_non_ascii() {
        assert!(validate_app_name("").is_err());
        assert!(validate_app_name(&"a".repeat(65)).is_err());
        assert!(validate_app_name("caf\u{e9}").is_err());
        assert!(validate_app_name("\u{ff0e}\u{ff0e}").is_err());
    }
}
