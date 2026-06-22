use std::os::macos::raw::stat;
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

struct AppState {
    counter: AtomicU64,
    stream: jetstream::stream::Stream,
}

async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
) -> actix_web::Result<HttpResponse> {
    let (response, session, msg_stream) = actix_ws::handle(&req, body)?;
    let i = state.counter.fetch_add(1, Ordering::SeqCst);
    println!("{} connections established in total", i);
    let uuid = uuid::Uuid::new_v4();
    let consumer_name = format!("ws-gateway-{}-{}", i, uuid);

    let consumer = state.stream.get_or_create_consumer(
        &consumer_name,
        PullConsumerConfig {
            durable_name: Some(consumer_name.clone()),
            filter_subject: "events_ordered".to_string(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        },
    ).await.expect("consumer setup failed");
    let messages = consumer.messages().await.expect("consumer stream failed");
    actix_web::rt::spawn(
        websocket_loop(messages, msg_stream, state, session, consumer_name)
    );

    Ok(response)
}

async fn websocket_loop(mut messages: Stream, mut msg_stream: MessageStream, state: Data<AppState>, mut session: Session, consumer_name: String){
    loop {
        tokio::select! {
                // NATS event -> WebSocket client
                Some(event) = messages.next() => {
                    match event {
                        Ok(text) => {
                            println!("{}", String::from_utf8(text.payload.to_vec()).unwrap());
                            text.ack().await.expect("ack failed");
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
    state.stream.delete_consumer(&consumer_name).await.expect("consumer delete failed");
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = async_nats::connect("localhost:4222").await.expect("NATS connect failed");
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


    let state = web::Data::new(AppState {
        counter: AtomicU64::new(0),
        stream,
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