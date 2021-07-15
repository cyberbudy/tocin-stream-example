use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;

use tonic::transport::Server;
use tokio::time::{sleep, Duration};
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use feeds::feeds_server::Feeds;
use feeds::feeds_server::FeedsServer;
use feeds::{Item, SubscribeResponse, SubscribeStreamRequest};

pub mod feeds {
    tonic::include_proto!("feeds");
}


#[derive(Debug)]
pub struct Shared {
    pub senders: HashMap<Uuid, mpsc::Sender<SubscribeResponse>>,
}

impl Shared {
    pub fn new() -> Self {
        Shared {
            senders: HashMap::new(),
        }
    }

    async fn broadcast(&self, msg: SubscribeResponse) {
        // To make our logic simple and consistency, we will broadcast to all
        // users which include msg sender.
        // On frontend, sender will send msg and receive its broadcasted msg
        // and then show his msg on frontend page.
        for (name, tx) in &self.senders {
            match tx.send(msg.clone()).await {
                Ok(_) => {}
                Err(_) => {
                    println!("[Broadcast] SendError: to {}, {:?}", name, msg)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct FeedsService {
    shared: Arc<RwLock<Shared>>,
}

impl FeedsService {
    pub fn new(shared: Arc<RwLock<Shared>>) -> Self {
        FeedsService { shared }
    }
}

#[tonic::async_trait]
impl Feeds for FeedsService {
    type SubscribeStreamStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send + Sync + 'static>>;

    async fn subscribe_stream(
        &self,
        request: Request<tonic::Streaming<SubscribeStreamRequest>>,
    ) -> Result<Response<Self::SubscribeStreamStream>, Status> {
        let request_id = Uuid::new_v4();
        let mut stream = request.into_inner();
        let (tx, mut rx) = mpsc::channel(1);
        let (stream_tx, stream_rx) = mpsc::channel(1);

        {
            self.shared.write().await.senders.insert(request_id, tx);
        }
        println!(
            "Current users size {:#?}",
            self.shared.read().await.senders.keys().len()
        );
        async_stream::try_stream! {
            while let Some(message) = stream.next().await {
            }
        };

        let shared_clone = self.shared.clone();

        // Item is not initialized. It's a first message
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match stream_tx.send(Ok(msg)).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[Remote] stream tx sending error. Remote {}", e);
                        shared_clone.write().await.senders.remove(&request_id);

                    }
                }
            };
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream_rx)
        )))
    }
}


pub async fn send_data(shared: Arc<RwLock<Shared>>) {
    loop {
        let message = SubscribeResponse {
            item: Some(Item {
                id:  "1".to_string(),
            }),
        };
        shared.read().await.broadcast(message).await;
        sleep(Duration::from_millis(10)).await;
    }
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();

    println!("FeedsServer listening on: {}", addr);
    let shared = Arc::new(RwLock::new(Shared::new()));

    let feeds = FeedsService::new(shared.clone());
    let feeds_server = FeedsServer::new(feeds);

    tokio::join!(
        send_data(shared.clone()),
        Server::builder()
            .add_service(feeds_server)
            .serve(addr)
    );

    Ok(())
}
