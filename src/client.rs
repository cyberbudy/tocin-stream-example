use std::error::Error;
use std::time::Duration;

use tokio::time;
use tonic::transport::Channel;
use tonic::Request;

use feeds::feeds_client::FeedsClient;
use feeds::{SubscribeStreamRequest};

pub mod feeds {
    tonic::include_proto!("feeds");
}

async fn run_stream(client: &mut FeedsClient<Channel>) -> Result<(), Box<dyn Error>> {
    let start = time::Instant::now();

    let outbound = async_stream::stream! {
        let mut interval = time::interval(Duration::from_secs(1));

        while let time = interval.tick().await {
            let elapsed = time.duration_since(start);
            let req = SubscribeStreamRequest {
                id: "1".to_string(),
            };

            yield req;
        }
    };

    let response = client.subscribe_stream(Request::new(outbound)).await?;
    let mut inbound = response.into_inner();
    let mut i = 0;

    while let Some(req) = inbound.message().await? {
        println!("Response = {:?}", req);
        i += 1;

        if i > 100 {
            break;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FeedsClient::connect("http://127.0.0.1:50051").await?;

    loop {
        println!("\n*** BIDIRECTIONAL STREAMING ***");
        run_stream(&mut client).await;
    }
    Ok(())
}
