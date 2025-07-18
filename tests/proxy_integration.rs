use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use rtc::{proxy::{self, ProxyConfig}, server};
use tonic::Request;

pub mod test {
    tonic::include_proto!("test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_forwards_requests() {
    let server_addr: SocketAddr = "127.0.0.1:50052".parse().unwrap();
    let proxy_addr: SocketAddr = "127.0.0.1:50053".parse().unwrap();
    let descriptor = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/echo_descriptor.bin"));

    let server_handle = tokio::spawn(server::serve(server_addr));

    // wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut routes = HashMap::new();
    routes.insert(
        "/test.EchoService/Echo".to_string(),
        format!("http://{}", server_addr).parse().unwrap(),
    );
    let config = ProxyConfig {
        descriptor_path: descriptor,
        listen: proxy_addr,
        default: format!("http://{}", server_addr).parse().unwrap(),
        routes,
    };
    let proxy_handle = tokio::spawn(proxy::serve(config));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = test::echo_service_client::EchoServiceClient::connect(
        format!("http://{}", proxy_addr),
    )
    .await
    .unwrap();
    let req = || Request::new(test::EchoRequest { message: "hello".into() });
    let resp1 = client.echo(req()).await.unwrap().into_inner();
    assert_eq!(resp1.message, "hello");

    // stop the upstream server so second call must hit the cache
    server_handle.abort();

    let resp2 = client.echo(req()).await.unwrap().into_inner();
    assert_eq!(resp2.message, "hello");

    proxy_handle.abort();
}
