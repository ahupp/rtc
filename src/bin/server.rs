use rtc::server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = "127.0.0.1:50052".parse()?;
    println!("Server listening on {}", addr);
    server::serve(addr).await
}
