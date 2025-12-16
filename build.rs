use std::env;

fn main() {
    let has_internal_dm = env::var("CARGO_FEATURE_INTERNAL_DM").is_ok();
    let has_default = env::var("CARGO_FEATURE_DEFAULT").is_ok();

    // Show warning if user tries to use internal-dm with default features
    if has_internal_dm && has_default {
        println!("cargo:warning=HINT: To exclude devicemapper dependency, use: cargo build --no-default-features --features internal-dm");
        println!("cargo:warning=Current build includes both internal and external devicemapper implementations");
    }
}
