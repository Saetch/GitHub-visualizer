use std::string::String;
use std::ops::Add;
use crate::jetstream::consumer::pull::Stream;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use actix_ws::{Message, MessageStream, Session};
use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull::Config as PullConsumerConfig};
use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use actix_web::web::Data;
use async_nats::jetstream::consumer::pull::{MessagesError, MessagesErrorKind};
use chrono::{DateTime, Utc};
use geojson::Feature;
use tokio::io::AsyncReadExt;
use tokio::sync::{RwLock};
use visualizer_protocol::GitEventMessage;

struct AppState {
    counter: AtomicU64,
    stream: jetstream::stream::Stream,
    delete_sender: tokio::sync::mpsc::Sender<String>,
}

async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<RwLock<AppState>>,
) -> actix_web::Result<HttpResponse> {
    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;
    let i = state.read().await.counter.fetch_add(1, Ordering::SeqCst);
    println!("{} connections established in total", i);
    let uuid = uuid::Uuid::new_v4();
    let consumer_name = format!("ws-gateway-{}-{}", i, uuid);

    let now = Utc::now();
    let start_time = now - Duration::from_mins(90) - Duration::from_hours(24*30*6);

    let index_to_start_at = find_correct_sequence_for_time(&state, start_time).await;

    let consumer = state.read().await.stream.get_or_create_consumer(
        &consumer_name,
        PullConsumerConfig {
            durable_name: Some(consumer_name.clone()),
            filter_subject: "events_ordered".to_string(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            deliver_policy: DeliverPolicy::ByStartSequence {
                start_sequence: index_to_start_at,
            },
            ..Default::default()
        },
    ).await.expect("consumer setup failed");
    let messages = consumer.messages().await.expect("consumer stream failed");
    actix_web::rt::spawn(
        websocket_loop(messages, msg_stream, state.read().await.delete_sender.clone(), session, consumer_name)
    );

    Ok(response)
}

async fn websocket_loop(mut messages: Stream, mut msg_stream: MessageStream, delete_sender: tokio::sync::mpsc::Sender<String>, mut session: Session, consumer_name: String){
    let mut looping = true; // first message successful. This is used in order to determine the real offset used for playback
    let mut first_message_time = Utc::now(); //will be overridden by the first message
    tokio::select! {
        string = get_next_message_async(&mut messages) => {
            let string = string.unwrap();
            let geojson_feature: Feature = string.parse().unwrap();
            let vis_me_prop = geojson_feature.property("visualizer_message").unwrap();
            let visualizer_msg: GitEventMessage = serde_json::from_value(vis_me_prop.clone()).unwrap();   //<-- evil unwraps
            first_message_time = match visualizer_msg {
                GitEventMessage::Placeholder { time, ..} => time
            };
            session.text(string).await.expect("send failed");
        }
        msg = msg_stream.next() => {
            match msg {
                Some(Ok(Message::Close(_))) | None => looping = false,
                Some(Err(e)) => {
                    eprintln!("WS error: {}", e);
                    looping = false;
                }
                _ => {}
            }
        }
    }
    let playback_start_time = Utc::now();

    while looping {
        tokio::select! {
                // NATS event -> WebSocket client
                message_string = get_next_timed_message_async(&mut messages, first_message_time, playback_start_time) => {
                    match message_string {
                        Ok(text) => {

                            session.text(text).await.expect("send failed");
                        }
                        Err(e) => {
                            eprintln!("NATS error: {}", e);
                            break;
                        },
                    }
                }

                // Handle close/ping from client
                msg = msg_stream.next() => {
                    match msg {
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Err(e)) => {
                            eprintln!("WS error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
    }
    let _ = session.close(None).await;
    delete_sender.send(consumer_name).await.expect("delete sender failed");
}

async fn get_next_timed_message_async(stream: &mut Stream, original_time: DateTime<Utc>, playback_start_time: DateTime<Utc>) -> Result<String, MessagesError>{
    if let Some(msg)= stream.next().await{
        let content = match msg {
            Ok(msg) => {
                let cont = String::from_utf8(msg.payload.to_vec());
                msg.ack().await.expect("Couldnt ack read message: panic ensues!");
                cont
            }
            Err(e) => {
                eprintln!("NATS error: {}", e);
                return Err(MessagesError::new(MessagesErrorKind::Other));
            }
        };
        let string = match content {
            Ok(content) => content,
            Err(e) => {
                eprintln!("WS error: {}", e);
                return Err(MessagesError::new(MessagesErrorKind::Other));
            }
        };

        let geojson_feature: Feature = string.parse().unwrap();
        let vis_me_prop = geojson_feature.property("visualizer_message").unwrap();
        let visualizer_message: GitEventMessage = serde_json::from_value(vis_me_prop.clone()).unwrap();

        let message_time = match visualizer_message {
            GitEventMessage::Placeholder { time, ..} => time
        };
        let passed_time = Utc::now() - playback_start_time;
        let supposed_passed_time = message_time - original_time;
        if let Ok(time_to_wait) = (supposed_passed_time-passed_time).to_std() {
            tokio::time::sleep(time_to_wait).await;
        } //will produce an Err if it is negative. But in this case we should just dispatch it right away

        return Ok(string);
    }


    Ok(String::from("placeholder. This should never happen."))
}

async fn get_next_message_async(stream: &mut Stream) -> Result<String, MessagesError>{
    if let Some(msg)= stream.next().await{
        let content =  String::from_utf8(msg?.payload.to_vec());
        let string = match content {
            Ok(content) => content,
            Err(e) => {
                eprintln!("WS error: {}", e);
                return Err(MessagesError::new(MessagesErrorKind::Other));
            }
        };
        return Ok(string);
    }

    eprintln!("No message received");
    Err(MessagesError::new(MessagesErrorKind::Other))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = async_nats::connect("nats:4222").await.expect("NATS connect failed");
    let js = jetstream::new(client);

    let stream = js.get_or_create_stream(StreamConfig {
        name: "DISPATCHER_ORDERED".to_string(),
        subjects: vec!["events_ordered".to_string()],
        storage: StorageType::File,
        retention: RetentionPolicy::Limits,
        discard: DiscardPolicy::Old,
        max_age: Duration::from_secs(2400 * 60 * 60),
        max_bytes: 15 * 1024 * 1024 * 1024,
        allow_direct: true,
        ..Default::default()
    }).await.expect("stream setup failed");
    let (delete_sender, mut delete_receiver) = tokio::sync::mpsc::channel(10);

    let state = web::Data::new(RwLock::new(AppState {
        counter: AtomicU64::new(0),
        stream,
        delete_sender,
    }));

    let state_clone = state.clone();
    tokio::spawn(async move {
        let state= state_clone;
        loop {
            let consumer_name = delete_receiver.recv().await.expect("delete receiver failed");
            let _ = state.read().await.stream.delete_consumer(&consumer_name).await;
        }
    });


    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/ws", web::get().to(ws_handler))
    })
    .bind("0.0.0.0:9001")?
    .run()
    .await
}


async fn find_correct_sequence_for_time(state: &Data<RwLock<AppState>>, target_time: DateTime<Utc>) -> u64 {
    let mut lock = state.write().await;
    let mut lowest_allowed_index = lock.stream.info().await.expect("stream info failed").state.first_sequence;
    let mut highest_allowed_index =lock.stream.info().await.expect("stream info failed").state.last_sequence;
    drop(lock);

    let mut mid = (highest_allowed_index + lowest_allowed_index) / 2;
    println!("mid: {}", mid);
    if mid == 0 {
        return 0;
    }
    let msg = state.read().await.stream.direct_get(mid).await.expect("stream info failed");
    let string = String::from_utf8(msg.payload.to_vec()).unwrap();
    let geojson_feature: Feature = string.parse().unwrap();
    let vis_me_prop = geojson_feature.property("visualizer_message").unwrap();
    let protocol_message: GitEventMessage = serde_json::from_value(vis_me_prop.clone()).unwrap();
    let mut time = match protocol_message {
        GitEventMessage::Placeholder { time, ..} => {
            time
        }
    };
    println!("{:?}", time);

    while !(time < target_time.add(Duration::from_secs(10)) && time.add(Duration::from_secs(10)) > target_time )  {
        if lowest_allowed_index > highest_allowed_index{
            println!("Running out of indices. Going with current target: {} - index {}", time, mid);
            break;
        }
        if time < target_time {
            lowest_allowed_index = mid + 1;
        } else {
            highest_allowed_index = mid - 1;
        }
        println!("index ranging from {} to {}", lowest_allowed_index, highest_allowed_index);
        mid = (highest_allowed_index + lowest_allowed_index) / 2;
        if mid == 0 {
            return 1;
        }
        let msg = state.read().await.stream.direct_get(mid).await.expect("stream info failed");
        let string = String::from_utf8(msg.payload.to_vec()).unwrap();
        let geojson_feature: Feature = string.parse().unwrap();
        let vis_me_prop = geojson_feature.property("visualizer_message").unwrap();
        let protocol_message: GitEventMessage = serde_json::from_value(vis_me_prop.clone()).unwrap();
        time = match protocol_message {
            GitEventMessage::Placeholder { time, ..} => {
                time
            }
        };
        println!("mid: {}", mid);

        println!("{:?}", time);
    }

    mid

}