//! Validation of relative logical file paths.

#[derive(Debug, thiserror::Error)]
#[error(
    "file paths must be nonempty relative paths without empty, dot, parent, backslash, or control components"
)]
pub struct PathError;

pub fn validate_relative_path(path: &str) -> Result<(), PathError> {
    if path.is_empty()
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains(':'))
    {
        return Err(PathError);
    }
    Ok(())
}
