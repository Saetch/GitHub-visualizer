use futures_util::{Stream, TryStreamExt};
use async_compression::tokio::bufread::GzipDecoder;
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::bytes::Bytes;
use tokio_util::io::StreamReader;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let date = "2025-03-05";
    let hour = "12";

    let url = format!("https://data.gharchive.org/{date}-{hour}.json.gz");

    let response = reqwest::get(&url).await.unwrap();
    let stream = response.bytes_stream();
    let stream = stream.map_err(|err| {
        io::Error::new(io::ErrorKind::Other, err)
    });
    let reader = StreamReader::new(stream);
    let decoder = GzipDecoder::new(reader);
    let mut lines = BufReader::new(decoder).lines();
    let client = async_nats::connect("localhost:4222").await.unwrap();

    while let Some(line) = lines.next_line().await.unwrap() {
        client.publish("events", line.into()).await.unwrap();
    }
    println!("Done");
}
