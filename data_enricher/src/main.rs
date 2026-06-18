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
use rand::random_range;
use visualizer_protocol::GitEventMessage;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("localhost:4222").await?;
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


    let dispatch_jetstream = async_nats::jetstream::new(client);
    dispatch_jetstream
        .get_or_create_stream(StreamConfig {
            name: "ENRICHED_UNORDERED".to_string(),
            subjects: vec!["events".to_string()],
            storage: StorageType::Memory,
            retention: RetentionPolicy::Interest,
            discard: DiscardPolicy::Old,
            max_age: Duration::from_secs(24 * 60 * 60),
            max_bytes: 150 * 1024 * 1024,
            ..Default::default()

        })
        .await
        .unwrap();

    while let Some(message) = messages.next().await {
        let message = message?;

        let message_string = String::from_utf8(message.payload.to_vec());
        println!(
            "Received message: {}",
            message_string.unwrap_or_else(|_| "Invalid UTF-8".to_string())
        );

        let ge_message = GitEventMessage::Placeholder {
            location_x: random_range(0.0..1.0),
            location_y: random_range(0.0..1.0),
            event_description: "Fake created event".to_string(),
        };

        let ack = jetstream.publish("events", serde_json::to_string(&ge_message)?.into()).await?;

        // Important: ack only after successful processing.
        message.ack().await.unwrap();
    }

    Ok(())
}