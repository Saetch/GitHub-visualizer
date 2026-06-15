use async_compression::tokio::bufread::GzipDecoder;
use async_nats::jetstream;
use async_nats::jetstream::stream::{
    Config as StreamConfig,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
};
use futures_util::TryStreamExt;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let now = std::time::SystemTime::now();
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
    let jetstream = async_nats::jetstream::new(client);
    jetstream
        .get_or_create_stream(StreamConfig {
            name: "GHARCHIVE".to_string(),
            subjects: vec!["events".to_string()],
            storage: StorageType::File,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            max_age: Duration::from_secs(24 * 60 * 60),
            max_bytes: 5 * 1024 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();

    while let Some(line) = lines.next_line().await.unwrap() {
        let ack = jetstream.publish("events", line.into()).await.unwrap();
        ack.await.unwrap();
    }
    println!("Done");
    let end = std::time::SystemTime::now();
    let elapsed = end.duration_since(now).unwrap();
    println!("Elapsed: {:?} ms", elapsed.as_millis());
}
