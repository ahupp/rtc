use std::{collections::HashMap, convert::Infallible, fs, net::SocketAddr, path::PathBuf, sync::Arc};

use hyper::{client::HttpConnector, service::service_fn, Body, Client, Request, Response, Uri};
use bytes::Bytes;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use prost_reflect::DescriptorPool;
use tokio::net::TcpListener;

pub struct ProxyConfig {
    pub descriptor_path: PathBuf,
    pub listen: SocketAddr,
    pub default: Uri,
    pub routes: HashMap<String, Uri>,
}

struct ProxyState {
    client: Client<HttpConnector, Body>,
    routes: HashMap<String, Uri>,
    default: Uri,
    pool: DescriptorPool,
    cache: Cache<Vec<u8>, Bytes>,
}

fn method_exists(pool: &DescriptorPool, path: &str) -> bool {
    let trimmed = path.trim_start_matches('/');
    if let Some((service, method)) = trimmed.split_once('/') {
        if let Some(svc) = pool.get_service_by_name(service) {
            return svc.methods().any(|m| m.name() == method);
        }
    }
    false
}

async fn handle(req: Request<Body>, state: Arc<ProxyState>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();

    if !method_exists(&state.pool, &path) {
        let resp = Response::builder().status(404).body(Body::from("unknown method")).unwrap();
        return Ok(resp);
    }

    let uri = state.routes.get(&path).unwrap_or(&state.default).clone();

    let (mut parts, body) = req.into_parts();
    let full_body = hyper::body::to_bytes(body).await.unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&full_body);
    let key: Vec<u8> = hasher.finalize().to_vec();

    if let Some(cached) = state.cache.get(&key).await {
        let resp = Response::builder()
            .status(200)
            .body(Body::from(cached))
            .unwrap();
        return Ok(resp);
    }

    let mut new_uri_parts = parts.uri.into_parts();
    new_uri_parts.scheme = uri.scheme().cloned();
    new_uri_parts.authority = uri.authority().cloned();
    parts.uri = Uri::from_parts(new_uri_parts).expect("valid uri");
    let new_req = Request::from_parts(parts, Body::from(full_body.clone()));

    match state.client.request(new_req).await {
        Ok(resp) => {
            let (mut parts, body) = resp.into_parts();
            let bytes = hyper::body::to_bytes(body).await.unwrap_or_default();
            state.cache.insert(key, bytes.clone()).await;
            let resp = Response::from_parts(parts, Body::from(bytes));
            Ok(resp)
        }
        Err(err) => {
            let resp = Response::builder()
                .status(500)
                .body(Body::from(format!("upstream error: {err}")))
                .unwrap();
            Ok(resp)
        }
    }
}

pub async fn serve(config: ProxyConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bytes = fs::read(config.descriptor_path)?;
    let pool = DescriptorPool::decode(bytes.as_slice())?;

    let client = Client::builder().http2_only(true).build_http();
    let cache = Cache::new(1024);

    let state = Arc::new(ProxyState {
        client,
        routes: config.routes,
        default: config.default,
        pool,
        cache,
    });

    let listener = TcpListener::bind(config.listen).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = hyper::server::conn::Http::new()
                .http2_only(true)
                .serve_connection(stream, service_fn(move |req| handle(req, state.clone())))
                .await
            {
                eprintln!("server error: {err}");
            }
        });
    }
}

