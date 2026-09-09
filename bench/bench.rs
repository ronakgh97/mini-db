use anyhow::Result;
use rand::{Rng, rng};

#[allow(unused)]
fn generate_random_kv() -> (Vec<u8>, Vec<u8>) {
    let mut key_buf = vec![0u8; 16];
    rng().fill_bytes(&mut key_buf);

    let mut value_buf = vec![0u8; 128];
    rng().fill_bytes(&mut value_buf);

    (key_buf, value_buf)
}

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}
