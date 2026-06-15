use futures_util::{Stream, StreamExt, TryStreamExt};
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::bytes::Bytes;
use tokio_util::io::StreamReader;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = async_nats::connect("localhost:4222").await.unwrap();

    let mut stream = client.subscribe("events").await.unwrap();
    while let Some(msg) = stream.next().await {
        println!("Received message: {:?}", msg);
    }
}
