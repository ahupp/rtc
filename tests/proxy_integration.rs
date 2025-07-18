use std::{fs, net::SocketAddr, path::PathBuf, time::Duration};

use tempfile::TempDir;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use hyper::{service::service_fn, Body, Request as HyperRequest, Response};
use prost::Message;
use rtc::proxy::{self, ProxyConfig};
use tokio::net::TcpListener;
use tonic::Request;

pub mod test {
    tonic::include_proto!("test");
}

async fn serve_with_trailer(
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    async fn handle(mut req: HyperRequest<Body>) -> Result<Response<Body>, hyper::Error> {
        let body = hyper::body::to_bytes(req.body_mut()).await?;
        let msg = test::EchoRequest::decode(&body[5..]).unwrap();
        let reply = test::EchoResponse {
            message: msg.message,
        };
        let mut resp_buf = Vec::new();
        resp_buf.push(0u8);
        let mut encoded = Vec::new();
        reply.encode(&mut encoded).unwrap();
        resp_buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        resp_buf.extend_from_slice(&encoded);

        let (mut tx, body_stream) = Body::channel();
        let mut trailers = HeaderMap::new();
        trailers.insert("x-rtc-dep", HeaderValue::from_static("dep1"));
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        tx.try_send_data(Bytes::from(resp_buf)).unwrap();
        tx.send_trailers(trailers).await.unwrap();

        Ok(Response::builder()
            .status(200)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(body_stream)
            .unwrap())
    }

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = hyper::server::conn::Http::new()
                .http2_only(true)
                .serve_connection(stream, service_fn(handle))
                .await
            {
                eprintln!("server error: {err}");
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_forwards_requests() {
    let server_addr: SocketAddr = "127.0.0.1:50052".parse().unwrap();
    let proxy_addr: SocketAddr = "127.0.0.1:50053".parse().unwrap();
    let descriptor = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/echo_descriptor.bin"));
    let tmp = TempDir::new().unwrap();
    let svc_dir = tmp.path().join("echo");
    fs::create_dir(&svc_dir).unwrap();
    fs::copy(&descriptor, svc_dir.join("descriptor.bin")).unwrap();
    fs::write(
        svc_dir.join("config.yaml"),
        format!("route: http://{}", server_addr),
    )
    .unwrap();

    let server_handle = tokio::spawn(serve_with_trailer(server_addr));

    // wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = ProxyConfig {
        descriptor_root: tmp.path().to_path_buf(),
        listen: proxy_addr,
        default: format!("http://{}", server_addr).parse().unwrap(),
    };
    let proxy_handle = tokio::spawn(proxy::serve(config));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client =
        test::echo_service_client::EchoServiceClient::connect(format!("http://{}", proxy_addr))
            .await
            .unwrap();
    let req = || {
        Request::new(test::EchoRequest {
            message: "hello".into(),
        })
    };
    let resp1 = client.echo(req()).await.unwrap();
    assert_eq!(resp1.get_ref().message, "hello");
    assert_eq!(resp1.metadata().get("x-rtc-dep").unwrap(), "dep1");

    // stop the upstream server so second call must hit the cache
    server_handle.abort();

    let resp2 = client.echo(req()).await.unwrap();
    assert_eq!(resp2.get_ref().message, "hello");
    assert_eq!(resp2.metadata().get("x-rtc-dep").unwrap(), "dep1");

    proxy_handle.abort();
}
