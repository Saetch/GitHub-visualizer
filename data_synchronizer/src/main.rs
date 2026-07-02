use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::ops::Add;
use std::str::FromStr;
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
use async_nats::jetstream::Message;
use chrono::{DateTime, Utc};
use futures_util::future::pending;
use geojson::Feature;
use visualizer_protocol::GitEventMessage;
use serde::Deserialize;
use tokio::time::{sleep_until, Instant};

#[derive(Deserialize, Debug)]
struct PartialPayload {
    created_at: DateTime<Utc>,
}

const HOLD_FOR: Duration = Duration::from_secs(248);

#[derive(Debug)]
struct BufferedMessage {
    event_time: i64,
    git_event_message: Feature,
    time_to_wait_for: Instant,
}

impl BufferedMessage {
    fn new(p0: Feature, p1: DateTime<Utc>, time_to_wait_for: Instant) -> Self {
        let event_time = p1.timestamp_millis();
        Self {
            event_time,
            git_event_message: p0,
            time_to_wait_for,
        }
    }
}

impl Eq for BufferedMessage {}

impl PartialEq<Self> for BufferedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.event_time.eq(&other.event_time)
    }
}

impl PartialOrd<Self> for BufferedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.event_time.partial_cmp(&other.event_time)
    }
}

impl Ord for BufferedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.event_time.cmp(&other.event_time)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("nats:4222").await?;
    let jetstream = jetstream::new(client.clone());

    // Ensure the stream exists. This is the storage layer.
    let stream = jetstream
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
        .await?;

    // Ensure a durable consumer exists. This is the read cursor.
    let consumer = stream
        .get_or_create_consumer(
            "synchronizer",
            PullConsumerConfig {
                durable_name: Some("synchronizer".to_string()),
                filter_subject: "events_unordered".to_string(),
                ack_policy: AckPolicy::Explicit,
                max_ack_pending: 2500,
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
            name: "DISPATCHER_ORDERED".to_string(),
            subjects: vec!["events_ordered".to_string()],
            storage: StorageType::File,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            allow_direct: true,
            max_age: Duration::from_secs(2400 * 60 * 60),
            max_bytes: 15 * 1024 * 1024 * 1024,
            ..Default::default()

        })
        .await
        .unwrap();


    let mut binary_buffer : BinaryHeap<Reverse<BufferedMessage>> = BinaryHeap::new();
    loop {
        let next_deadline = binary_buffer.peek().map(|Reverse(buffered_message)| buffered_message.time_to_wait_for);
        let now = Instant::now();
        tokio::select! {
            _ = sleep_until_optional(next_deadline)=> {

                let buffered_message = binary_buffer.pop().unwrap().0;
                let payload = buffered_message.git_event_message;
                dispatch_jetstream.publish("events_ordered", serde_json::to_string(&payload)?.into()).await?;
                println!("Dispatched message: {:?}", payload);
            },


            msg  = messages.next() => {
                if let Some(message) = msg {
                    let message = message?;
                    let message_string = String::from_utf8(message.payload.to_vec());

                    let geojson_feature: Feature = message_string?.parse()?;
                    let vis_me_prop = geojson_feature.property("visualizer_message").unwrap();
                    let encoded_message: GitEventMessage = serde_json::from_value(vis_me_prop.clone())?;
                    let time_of_event = match encoded_message{
                        GitEventMessage::Placeholder { time, .. } => time,
                        _ => continue,
                    };
                    let time_plus_wait_time = Instant::now().add(HOLD_FOR);
                    let buffered_message = BufferedMessage::new(geojson_feature, time_of_event, time_plus_wait_time);
                    binary_buffer.push(Reverse(buffered_message));
                    message.ack().await;
                }else{
                    println!("No more messages!");
                    break;
                }
            }
            _ = sleep_until(Instant::now().add(Duration::from_secs(5))) => {
                println!("I am alive!");
            }
        }
    }

    Ok(())
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}