use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use rtc::proxy::{self, ProxyConfig};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Root directory containing service descriptors
    #[arg(long)]
    descriptor_root: PathBuf,

    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    /// Default upstream gRPC endpoint (e.g. http://localhost:50052)
    #[arg(long)]
    default: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let config = ProxyConfig {
        descriptor_root: args.descriptor_root,
        listen: args.listen,
        default: args.default.parse()?,
    };

    proxy::serve(config).await
}
