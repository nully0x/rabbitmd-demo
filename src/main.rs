use dotenv::dotenv;
use futures_util::StreamExt;
use lapin::{
    options::*, types::FieldTable, Connection, ConnectionProperties, Result as LapinResult,
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::env;
use tokio;

#[derive(Debug, Deserialize, Serialize)]
struct WebhookPayload {
    event_type: String,
    #[serde(rename = "repoUrl")]
    repo_url: String,
    branch: String,
    #[serde(rename = "commitSha")]
    commit_sha: String,
}

async fn process_message(payload: WebhookPayload) {
    info!("Processing webhook payload: {:?}", payload);
    println!("Received data from queue:");
    println!("Event Type: {}", payload.event_type);
    println!("Repository URL: {}", payload.repo_url);
    println!("Branch: {}", payload.branch);
    println!("Commit SHA: {}", payload.commit_sha);
    //we can futher process payload here
}

async fn consume_messages() -> LapinResult<()> {
    dotenv().ok();
    let amqp_url = env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost".into());
    let queue_name = env::var("QUEUE_NAME").unwrap_or_else(|_| "backend_core_queue".into());

    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let _queue = channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                durable: true, // Tis ensures the queue survives broker restarts
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    info!("Waiting for messages. To exit press CTRL+C");

    let mut consumer = channel
        .basic_consume(
            &queue_name,
            "backend_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            match serde_json::from_slice(&delivery.data) {
                Ok(payload) => {
                    let payload: WebhookPayload = payload;
                    process_message(payload).await;
                    delivery.ack(BasicAckOptions::default()).await?;
                }
                Err(e) => {
                    error!("Failed to deserialize payload: {:?}", e);
                    println!(
                        "Received invalid data from queue: {:?}",
                        String::from_utf8_lossy(&delivery.data)
                    );
                    delivery.nack(BasicNackOptions::default()).await?;
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> LapinResult<()> {
    env_logger::init();
    consume_messages().await
}
