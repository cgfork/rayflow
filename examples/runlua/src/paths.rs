use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use catalog::{directory::DIRECTORY_MANIFEST, path::validate_relative_path};

pub fn relative(name: &str, member: Option<&str>) -> mlua::Result<String> {
    let path = match member {
        Some(member) => format!("{name}/{member}"),
        None => name.to_owned(),
    };
    validate_relative_path(&path).map_err(mlua::Error::external)?;
    if path
        .split('/')
        .any(|component| component == DIRECTORY_MANIFEST)
    {
        return Err(mlua::Error::runtime("reserved directory manifest name"));
    }
    Ok(path)
}

pub fn existing(root: &Path, relative: &str) -> mlua::Result<Option<PathBuf>> {
    validate_relative_path(relative).map_err(mlua::Error::external)?;
    require_directory(root)?;
    let mut path = root.to_path_buf();
    let mut parts = relative.split('/').peekable();
    while let Some(part) = parts.next() {
        path.push(part);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() || (parts.peek().is_none() && metadata.is_file()) => {
            }
            Ok(_) => return Err(invalid_path(&path)),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(mlua::Error::external(error)),
        }
    }
    Ok(Some(path))
}

pub fn output(root: &Path, relative: &str, directory: bool) -> mlua::Result<PathBuf> {
    require_directory(root)?;
    let mut path = root.to_path_buf();
    let mut parts = relative.split('/').peekable();
    while let Some(part) = parts.next() {
        path.push(part);
        let needs_directory = directory || parts.peek().is_some();
        match fs::symlink_metadata(&path) {
            Ok(metadata) if needs_directory && metadata.is_dir() => {}
            Ok(metadata) if !needs_directory && metadata.is_file() => {}
            Ok(_) => return Err(invalid_path(&path)),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if needs_directory {
                    fs::create_dir(&path).map_err(mlua::Error::external)?;
                }
            }
            Err(error) => return Err(mlua::Error::external(error)),
        }
    }
    Ok(path)
}

fn require_directory(path: &Path) -> mlua::Result<()> {
    if !fs::symlink_metadata(path)
        .map_err(mlua::Error::external)?
        .is_dir()
    {
        return Err(invalid_path(path));
    }
    Ok(())
}

fn invalid_path(path: &Path) -> mlua::Error {
    mlua::Error::runtime(format!("invalid artifact path: {}", path.display()))
}
