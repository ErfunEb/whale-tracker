use dotenv::dotenv;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();

    let ws_rpc_url =
        std::env::var("WS_RPC_URL").map_err(|_| "Environment variable WS_RPC_URL is not set")?;

    println!("WS RPC URL: {}", ws_rpc_url);

    Ok(())
}
