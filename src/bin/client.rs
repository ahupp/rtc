use tonic::Request;

pub mod test {
    tonic::include_proto!("test");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = test::echo_service_client::EchoServiceClient::connect("http://127.0.0.1:50052").await?;
    let request = Request::new(test::EchoRequest { message: "hello".into() });
    let response = client.echo(request).await?.into_inner();
    println!("Response: {}", response.message);
    Ok(())
}
