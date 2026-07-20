use std::env;
use async_compression::tokio::bufread::GzipDecoder;
use async_nats::jetstream;
use async_nats::jetstream::stream::{
    Config as StreamConfig,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
};
use futures_util::TryStreamExt;
use std::time::Duration as StdDuration;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;
use chrono::{Local, Timelike, Utc};
use tokio::time::sleep;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut previous_stamp:String = "".to_string();
    let mut completed = false;
    let nats_target= env::var("NATS_TARGET").unwrap_or("nats:4222".to_string());
    let mut total_events_read :u128 = 0;
    loop {
        let current_stamp = last_completed_gharchive_hour();
        if current_stamp==previous_stamp{
            sleep(StdDuration::from_mins(5)).await;
        }else { completed = false; }
        if completed{
            continue;
        }
        previous_stamp = current_stamp;
        let now = Utc::now();
        let url = format!("https://data.gharchive.org/{previous_stamp}.json.gz");
        let now_local = Local::now();
        let now_utc = Utc::now();

        println!("Local time: {}", now_local.format("%Y-%m-%d %H:%M:%S %Z"));
        println!("UTC time:   {}", now_utc.format("%Y-%m-%d %H:%M:%S UTC"));
        println!("Downloading {} ...", &url);
        let response = reqwest::get(&url).await;
        if response.is_err() {
            println!("Failed to download {}, waiting 5 minutes", &url);
            continue;
        }else {
            println!("Connection established to {} successfully", &url);
        }
        let response = response.unwrap();
        println!("Response code: {}", response.status());
        if response.status().is_success(){
            println!("Downloaded {} successfully", &url);
        }else {
            println!("Failed to download {}, waiting 5 minutes", &url);
            continue;
        }
        let stream = response.bytes_stream();
        let stream = stream.map_err(|err| {
            io::Error::new(io::ErrorKind::Other, err)
        });
        let reader = StreamReader::new(stream);
        let decoder = GzipDecoder::new(reader);
        let mut lines = BufReader::new(decoder).lines();
        let client = async_nats::connect(nats_target.to_string()).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);
        jetstream
            .get_or_create_stream(StreamConfig {
                name: "GHARCHIVE".to_string(),
                subjects: vec!["events".to_string()],
                storage: StorageType::File,
                retention: RetentionPolicy::Limits,
                discard: DiscardPolicy::Old,
                max_age: StdDuration::from_secs(24 * 60 * 60),
                max_bytes: 5 * 1024 * 1024 * 1024,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut line_nmbr = 0;
        while let Some(line) = lines.next_line().await.unwrap() {
            let ack = jetstream.publish("events", line.into()).await.unwrap();
            line_nmbr += 1;
            if line_nmbr % 1000 == 0 {
                println!("Processed {} lines", line_nmbr);
            }
        }
        println!("Done");

        let end = Utc::now();
        let elapsed = end.signed_duration_since(now);
        println!("Elapsed: {:?}", elapsed);
        println!("Processed {} lines", line_nmbr);
        total_events_read += line_nmbr as u128;
        println!("Total events processed: {}", total_events_read);
        completed = true;
        sleep(StdDuration::from_mins(5)).await
    }


}



fn last_completed_gharchive_hour() -> String {
    let now = Utc::now();

    let last_hour = now
        .with_minute(0).unwrap()
        .with_second(0).unwrap()
        .with_nanosecond(0).unwrap()
        - StdDuration::from_hours(24 * 30 * 16);

    format!(
        "{}-{}",
        last_hour.format("%Y-%m-%d"),
        last_hour.hour()
    )
}
