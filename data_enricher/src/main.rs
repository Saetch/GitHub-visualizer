use std::ops::Add;
use async_nats::jetstream;
use async_nats::jetstream::consumer::{
    AckPolicy,
    DeliverPolicy,
    pull::Config as PullConsumerConfig,
};
use async_nats::jetstream::stream::{
    Config as StreamConfig,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
};
use futures_util::StreamExt;
use std::time::Duration;
use chrono::{DateTime, Timelike, Utc};
use geojson::GeometryValue::Point;
use geojson::{Feature, JsonObject, PointType};
use rand::random_range;
use visualizer_protocol::GitEventMessage;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct PartialPayload {
    created_at: DateTime<Utc>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut message_nmbr = 0;
    let client = async_nats::connect("nats:4222").await?;
    let jetstream = jetstream::new(client.clone());

    // Ensure the stream exists. This is the storage layer.
    let stream = jetstream
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
        .await?;

    //sleep for 1 second
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Ensure a durable consumer exists. This is the read cursor.
    let consumer = stream
        .get_or_create_consumer(
            "gharchive-worker",
            PullConsumerConfig {
                durable_name: Some("gharchive-worker".to_string()),
                filter_subject: "events".to_string(),
                ack_policy: AckPolicy::Explicit,
                max_ack_pending: 500,
                ack_wait: Duration::from_secs(60),
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            },
        )
        .await?;

    let mut messages = consumer.messages().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let dispatch_jetstream = async_nats::jetstream::new(client);
    dispatch_jetstream
        .get_or_create_stream(StreamConfig {
            name: "ENRICHED_UNORDERED".to_string(),
            subjects: vec!["events_unordered".to_string()],
            storage: StorageType::File,
            retention: RetentionPolicy::Interest,
            discard: DiscardPolicy::Old,
            max_age: Duration::from_secs(24 * 60 * 60),
            max_bytes: 150 * 1024 * 1024,
            ..Default::default()

        })
        .await
        .unwrap();

    let client = reqwest::Client::new();
    while let Some(message) = messages.next().await {
        let message = message?;

        let partial_payload: PartialPayload = serde_json::from_slice(&message.payload)?;
        let time_of_event = partial_payload.created_at;
        let guessed_time = time_of_event.add(Duration::from_millis(random_range(..1000)));
        let utc_hour = guessed_time.hour();
        let estimated_data_string = client.get(format!("http://sampler:9003/random_distributed_point?utc_hour={}", utc_hour)).send().await?.text().await?;
        let parts: Vec<&str> = estimated_data_string.split(',').collect();
        let lon = parts[0];
        let lat = parts[1];
        let iso2 = parts[2];
        let mut country = parts[3].to_string();
        if parts.len() > 4 {
            for i in 4..parts.len() {
                country.push_str(&format!(",{}", parts[i]));
            }
        }
        let ge_message = GitEventMessage::Placeholder {
            event_description: "Fake created event".to_string(),
            time: guessed_time,
            iso2: iso2.to_string(),
            country
        };
        let geometry = geojson::Geometry::new(Point { coordinates: PointType::from([lon.parse::<f64>()?, lat.parse::<f64>()?]) });
        let mut props = JsonObject::new();
        props.insert("visualizer_message".to_string(), serde_json::to_value(ge_message)?);
        //create a geojson Feature with the event data
        let feature = Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(props),
            foreign_members: None,
        };

        let ack = dispatch_jetstream.publish("events_unordered", serde_json::to_string(&feature)?.into()).await?;

        // Important: ack only after successful processing.
        message.ack().await.unwrap();
        message_nmbr += 1;
        println!("Processed message {}.", message_nmbr);
    }

    Ok(())
}