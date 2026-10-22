//! Validation of file names received from the network.
//!
//! A received name is attacker-controlled input that ends up in a filesystem
//! path. The rule here is deliberately strict: a name must be a single, plain
//! path component. Anything that could be interpreted as a directory
//! separator, a parent reference, an absolute path, or a drive prefix is
//! rejected outright rather than "cleaned", because sanitizing is where
//! traversal bugs usually hide.

use std::path::{Component, Path};

use crate::protocol::MAX_NAME_LEN;

/// Name of the hidden directory (inside the destination) holding in-progress
/// `.part` and `.state` files. Received names may not collide with it.
pub const PARTIAL_DIR: &str = ".rft-partial";

/// Why a file name was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("file name is empty")]
    Empty,
    #[error("file name is longer than {MAX_NAME_LEN} bytes")]
    TooLong,
    #[error("file name contains a forbidden character {0:?}")]
    ForbiddenChar(char),
    #[error("file name is not a single plain path component")]
    NotPlain,
    #[error("file name is reserved")]
    Reserved,
}

/// Accept `name` only if it is safe to join onto the destination directory.
pub fn validate_file_name(name: &str) -> Result<&str, NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }
    if name.len() > MAX_NAME_LEN {
        return Err(NameError::TooLong);
    }
    if let Some(c) = name
        .chars()
        .find(|&c| c == '/' || c == '\\' || c == ':' || c.is_control())
    {
        return Err(NameError::ForbiddenChar(c));
    }
    if name == "." || name == ".." {
        return Err(NameError::NotPlain);
    }
    if name == PARTIAL_DIR {
        return Err(NameError::Reserved);
    }
    // Defense in depth: whatever the platform's path parser thinks, the name
    // must be exactly one normal component equal to itself.
    let mut comps = Path::new(name).components();
    match (comps.next(), comps.next()) {
        (Some(Component::Normal(c)), None) if c == name => Ok(name),
        _ => Err(NameError::NotPlain),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_names() {
        for ok in [
            "a",
            "report.pdf",
            "archive.tar.gz",
            ".bashrc",
            "with space",
            "日本語.txt",
            "..hidden",
        ] {
            assert_eq!(validate_file_name(ok), Ok(ok), "{ok}");
        }
    }

    #[test]
    fn rejects_traversal_and_separators() {
        for bad in [
            "",
            ".",
            "..",
            "../etc/passwd",
            "../../x",
            "a/b",
            "/etc/passwd",
            "a\\b",
            "..\\..\\windows",
            "C:evil",
            "nul\0byte",
            "new\nline",
            PARTIAL_DIR,
        ] {
            assert!(validate_file_name(bad).is_err(), "{bad:?} was accepted");
        }
        assert_eq!(
            validate_file_name(&"x".repeat(256)),
            Err(NameError::TooLong)
        );
        assert!(validate_file_name(&"x".repeat(255)).is_ok());
    }
}
