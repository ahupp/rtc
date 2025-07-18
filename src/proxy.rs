use std::{
    collections::HashMap, convert::Infallible, fs, net::SocketAddr, path::PathBuf, sync::Arc,
};

use bytes::Bytes;
use hyper::{client::HttpConnector, service::service_fn, Body, Client, Request, Response, Uri};
use moka::future::Cache;

use std::time::SystemTime;

#[derive(Clone)]
struct CacheEntry {
    body: Bytes,
    service: String,
    method: String,
    created: SystemTime,
}
use prost::Message;
use prost_reflect::DescriptorPool;
use prost_types::FileDescriptorSet;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;

pub struct ProxyConfig {
    pub descriptor_root: PathBuf,
    pub listen: SocketAddr,
    pub default: Uri,
}

struct ProxyState {
    client: Client<HttpConnector, Body>,
    routes: HashMap<String, Uri>,
    default: Uri,
    pool: DescriptorPool,
    cache: Cache<Vec<u8>, CacheEntry>,
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
    let trimmed = path.trim_start_matches('/');
    let (service, method) = trimmed
        .split_once('/')
        .expect("validated service and method");
    let service = service.to_string();
    let method = method.to_string();

    if !method_exists(&state.pool, &path) {
        let resp = Response::builder()
            .status(404)
            .body(Body::from("unknown method"))
            .unwrap();
        return Ok(resp);
    }

    let uri = state.routes.get(&path).unwrap_or(&state.default).clone();

    let (mut parts, body) = req.into_parts();
    let full_body = hyper::body::to_bytes(body).await.unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&full_body);
    let key: Vec<u8> = hasher.finalize().to_vec();

    let client = state.client.clone();
    let entry_result = state
        .cache
        .try_get_with_by_ref(&key, async move {
            let mut new_parts = parts;
            let mut new_uri_parts = new_parts.uri.into_parts();
            new_uri_parts.scheme = uri.scheme().cloned();
            new_uri_parts.authority = uri.authority().cloned();
            new_parts.uri = Uri::from_parts(new_uri_parts).expect("valid uri");
            let new_req = Request::from_parts(new_parts, Body::from(full_body.clone()));

            match client.request(new_req).await {
                Ok(resp) => {
                    let (_parts, body) = resp.into_parts();
                    let bytes = hyper::body::to_bytes(body).await.unwrap_or_default();
                    Ok(CacheEntry {
                        body: bytes,
                        service: service.clone(),
                        method: method.clone(),
                        created: SystemTime::now(),
                    })
                }
                Err(err) => Err(err),
            }
        })
        .await;

    match entry_result {
        Ok(cached) => {
            let resp = Response::builder()
                .status(200)
                .body(Body::from(cached.body.clone()))
                .unwrap();
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

#[derive(Deserialize)]
struct ServiceYaml {
    route: String,
}

pub async fn serve(config: ProxyConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut pool = DescriptorPool::new();
    let mut routes = HashMap::new();

    for entry in fs::read_dir(&config.descriptor_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let dir_path = entry.path();
        let descriptor_path = dir_path.join("descriptor.bin");
        let yaml_path = dir_path.join("config.yaml");
        if !descriptor_path.exists() || !yaml_path.exists() {
            continue;
        }

        let bytes = fs::read(&descriptor_path)?;
        pool.decode_file_descriptor_set(bytes.as_slice())?;

        let yaml_str = fs::read_to_string(&yaml_path)?;
        let svc: ServiceYaml = serde_yaml::from_str(&yaml_str)?;
        let uri: Uri = svc.route.parse()?;

        let fds: FileDescriptorSet = FileDescriptorSet::decode(bytes.as_slice())?;
        for file in &fds.file {
            let package = file.package.as_deref().unwrap_or("");
            for service in &file.service {
                let svc_name = service.name.as_deref().unwrap_or("");
                let full_service = if package.is_empty() {
                    svc_name.to_string()
                } else {
                    format!("{}.{}", package, svc_name)
                };
                for method in &service.method {
                    let method_name = method.name.as_deref().unwrap_or("");
                    let path = format!("/{}/{}", full_service, method_name);
                    routes.insert(path, uri.clone());
                }
            }
        }
    }

    let client = Client::builder().http2_only(true).build_http();
    let cache = Cache::new(1024);

    let state = Arc::new(ProxyState {
        client,
        routes,
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
