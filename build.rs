use std::env;
use std::fs;
use std::path::PathBuf;

include!("build/key_limits.rs");

const SUPPORTED_KEY_BYTES: &[usize] = &[
    8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, 62, 63, 64,
];

fn supported_feature_list() -> String {
    SUPPORTED_KEY_BYTES
        .iter()
        .map(|b| format!("key-{b}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Détecte la taille de clé à partir des features Cargo.
/// Retourne Some(bytes) si une feature key-XX est activée, None sinon.
fn key_bytes_from_feature() -> Option<usize> {
    let mut found: Option<usize> = None;

    for bytes in SUPPORTED_KEY_BYTES {
        let env_name = format!("CARGO_FEATURE_KEY_{bytes}");
        if env::var(env_name).is_ok() {
            if found.is_some() {
                panic!(
                    "Plusieurs features key-XX activées en même temps. \
                     Activez-en une seule parmi: {}",
                    supported_feature_list()
                );
            }
            found = Some(*bytes);
        }
    }

    found
}

fn main() {
    // Priorité : 1) Feature Cargo  2) Variable d'environnement  3) Valeur par défaut
    let key_bytes = if let Some(bytes) = key_bytes_from_feature() {
        bytes
    } else {
        match env::var("ROLLBLOCK_KEY_BYTES") {
            Ok(raw) => raw
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("ROLLBLOCK_KEY_BYTES must be an integer (got `{raw}`)")),
            Err(env::VarError::NotPresent) => MIN_KEY_BYTES,
            Err(err) => panic!("failed to read ROLLBLOCK_KEY_BYTES: {err}"),
        }
    };

    if key_bytes < MIN_KEY_BYTES {
        panic!("ROLLBLOCK_KEY_BYTES must be at least {MIN_KEY_BYTES} (got {key_bytes})");
    }
    if key_bytes > MAX_KEY_BYTES {
        panic!("ROLLBLOCK_KEY_BYTES must not exceed {MAX_KEY_BYTES} (got {key_bytes})");
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let dest = out_dir.join("key_width.rs");
    let contents = format!(
        "/// Default number of bytes for store keys.\n\
         ///\n\
         /// Set at compile time via Cargo feature (key-8..key-64) \n\
         /// or `ROLLBLOCK_KEY_BYTES` environment variable (defaults to {MIN_KEY_BYTES}).\n\
         pub const DEFAULT_KEY_BYTES: usize = {key_bytes};\n"
    );
    fs::write(&dest, contents).expect("failed to write generated key width");

    // Recompiler si une de ces variables change
    println!("cargo:rerun-if-env-changed=ROLLBLOCK_KEY_BYTES");
    for bytes in SUPPORTED_KEY_BYTES {
        println!("cargo:rerun-if-env-changed=CARGO_FEATURE_KEY_{bytes}");
    }
}
