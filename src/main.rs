use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use clap::Parser;
use rtc::proxy::{self, ProxyConfig};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to binary FileDescriptorSet
    #[arg(long)]
    descriptor: PathBuf,

    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    /// Default upstream gRPC endpoint (e.g. http://localhost:50052)
    #[arg(long)]
    default: String,

    /// Mapping from fully qualified method name to upstream URI (e.g. package.Service/Method=http://host:port)
    #[arg(long)]
    route: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let mut routes = HashMap::new();
    for r in args.route {
        if let Some((method, uri)) = r.split_once('=') {
            routes.insert(format!("/{}", method.trim_start_matches('/')), uri.parse()?);
        }
    }

    let config = ProxyConfig {
        descriptor_path: args.descriptor,
        listen: args.listen,
        default: args.default.parse()?,
        routes,
    };

    proxy::serve(config).await
}
