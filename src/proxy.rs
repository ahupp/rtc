use std::{
    collections::HashMap, convert::Infallible, fs, net::SocketAddr, path::PathBuf, sync::Arc,
};

use bytes::Bytes;
use http::HeaderMap;
use hyper::{client::HttpConnector, service::service_fn, Body, Client, Request, Response, Uri};
use moka::future::Cache;

use std::time::SystemTime;
use tokio::sync::Mutex;

#[derive(Clone)]
struct CacheEntry {
    body: Bytes,
    service: String,
    method: String,
    created: SystemTime,
    deps: Vec<String>,
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
    deps: tokio::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>,
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

async fn collect_body_and_trailers(mut body: Body) -> (Bytes, Option<HeaderMap>) {
    use hyper::body::HttpBody;
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        if let Ok(chunk) = chunk {
            bytes.extend_from_slice(&chunk);
        } else {
            break;
        }
    }
    let trailers = body.trailers().await.ok().flatten();
    (Bytes::from(bytes), trailers)
}

async fn response_with_trailers(body: Bytes, deps: &[String]) -> Response<Body> {
    let (mut tx, body_stream) = Body::channel();
    let mut trailers = HeaderMap::new();
    for dep in deps {
        if let Ok(val) = dep.parse() {
            trailers.append("x-rtc-dep", val);
        }
    }
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    let _ = tx.try_send_data(body);
    let _ = tx.send_trailers(trailers).await;
    Response::builder().status(200).body(body_stream).unwrap()
}

async fn handle(req: Request<Body>, state: Arc<ProxyState>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();
    let trimmed = path.trim_start_matches('/');
    let (service, method) = match trimmed.split_once('/') {
        Some((svc, m)) => (svc.to_string(), m.to_string()),
        None => {
            let resp = Response::builder()
                .status(404)
                .body(Body::from("malformed path"))
                .unwrap();
            return Ok(resp);
        }
    };

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
    let state_clone = state.clone();
    let key_for_map = key.clone();
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
                    let (bytes, trailers) = collect_body_and_trailers(body).await;
                    let cache_key = key_for_map.clone();
                    let deps: Vec<String> = trailers
                        .as_ref()
                        .map(|t| {
                            t.get_all("x-rtc-dep")
                                .iter()
                                .filter_map(|v| v.to_str().ok().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    if !deps.is_empty() {
                        let mut map = state_clone.deps.lock().await;
                        for dep in &deps {
                            map.entry(dep.clone()).or_default().push(cache_key.clone());
                        }
                    }
                    Ok(CacheEntry {
                        body: bytes,
                        service: service.clone(),
                        method: method.clone(),
                        created: SystemTime::now(),
                        deps,
                    })
                }
                Err(err) => Err(err),
            }
        })
        .await;

    match entry_result {
        Ok(cached) => {
            let resp = response_with_trailers(cached.body.clone(), &cached.deps).await;
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
        deps: Mutex::new(HashMap::new()),
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
