use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use actix_ws::{Message};
use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, pull::Config as PullConsumerConfig};
use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType};
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

struct AppState {
    tx: broadcast::Sender<String>,
}

async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
) -> actix_web::Result<HttpResponse> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;

    let mut rx = state.tx.subscribe();

    actix_web::rt::spawn(async move {
        loop {
            tokio::select! {
                // NATS event -> WebSocket client
                event = rx.recv() => {
                    match event {
                        Ok(text) => {
                            if session.text(text).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            eprintln!("Client lagged, dropped {} messages", n);
                        }
                        Err(_) => break,
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
    });

    Ok(response)
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
        ..Default::default()
    }).await.expect("stream setup failed");

    let consumer = stream.get_or_create_consumer(
        "ws-gateway",
        PullConsumerConfig {
            durable_name: Some("ws-gateway".to_string()),
            filter_subject: "events_ordered".to_string(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        },
    ).await.expect("consumer setup failed");

    let (tx, _rx) = broadcast::channel::<String>(1024);
    let tx = Arc::new(tx);

    // NATS pump task
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut messages = consumer.messages().await.expect("consumer stream failed");
            while let Some(Ok(msg)) = messages.next().await {
                if let Ok(text) = String::from_utf8(msg.payload.to_vec()) {
                    let _ = tx.send(text);
                }
                let _ = msg.ack().await;
            }
        });
    }

    let state = web::Data::new(AppState {
        tx: (*tx).clone(),
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