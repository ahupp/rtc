use tonic::{transport::Server, Request, Response, Status};

pub mod test {
    tonic::include_proto!("test");
}

#[derive(Default)]
pub struct MyService;

#[tonic::async_trait]
impl test::echo_service_server::EchoService for MyService {
    async fn echo(
        &self,
        request: Request<test::EchoRequest>,
    ) -> Result<Response<test::EchoResponse>, Status> {
        let message = request.into_inner().message;
        let reply = test::EchoResponse { message };
        Ok(Response::new(reply))
    }
}

pub async fn serve(addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::builder()
        .add_service(test::echo_service_server::EchoServiceServer::new(MyService::default()))
        .serve(addr)
        .await?;
    Ok(())
}

