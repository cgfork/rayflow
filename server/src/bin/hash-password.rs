//! Hash a local account password read from standard input, never a CLI argument.
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use std::io::{self, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut password = String::new();
    io::stdin().take(4098).read_to_string(&mut password)?;
    let password = password
        .strip_suffix('\n')
        .unwrap_or(&password)
        .trim_end_matches('\r');
    if password.is_empty() || password.len() > 4096 {
        return Err("password must contain 1–4096 bytes".into());
    }
    let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes())
        .map_err(|error| error.to_string())?;
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| error.to_string())?;
    println!("{hash}");
    Ok(())
}
