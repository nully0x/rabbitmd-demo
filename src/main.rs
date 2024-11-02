use dotenv::dotenv;
use futures_util::StreamExt;
use lapin::{
    options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Result as LapinResult,
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Deserialize, Serialize)]
struct TestResultPayload {
    #[serde(rename = "commitSha")]
    commit_sha: String,
    result: TestResult,
}

#[derive(Debug, Deserialize, Serialize)]
struct TestResult {
    event_type: String,
    success: bool,
    output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

async fn process_webhook(payload: WebhookPayload) {
    info!("Processing webhook payload: {:?}", payload);
    println!("Received data from backend_core_queue:");
    println!("Event Type: {}", payload.event_type);
    println!("Repository URL: {}", payload.repo_url);
    println!("Branch: {}", payload.branch);
    println!("Commit SHA: {}", payload.commit_sha);
}

async fn process_test_result(payload: TestResultPayload) {
    info!("Processing test result payload: {:?}", payload);
    println!("Received data from test_results_queue:");
    println!("Commit SHA: {}", payload.commit_sha);
    println!("Test Result:");
    println!("  Event Type: {}", payload.result.event_type);
    println!("  Success: {}", payload.result.success);
    println!("  Output: {}", payload.result.output);
    if let Some(error) = payload.result.error_message {
        println!("  Error Message: {}", error);
    }
}

async fn consume_messages(
    channel: &Channel,
    queue_name: &str,
    consumer_tag: &str,
) -> LapinResult<()> {
    let _queue = channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    info!(
        "Waiting for messages on {}. To exit press CTRL+C",
        queue_name
    );

    let mut consumer = channel
        .basic_consume(
            queue_name,
            consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            match serde_json::from_slice::<Value>(&delivery.data) {
                Ok(json_value) => {
                    if queue_name == "backend_core_queue" {
                        if let Ok(payload) = serde_json::from_value::<WebhookPayload>(json_value) {
                            process_webhook(payload).await;
                        } else {
                            error!("Failed to parse WebhookPayload");
                        }
                    } else if queue_name == "test_results_queue" {
                        if let Ok(payload) =
                            serde_json::from_value::<TestResultPayload>(json_value.clone())
                        {
                            process_test_result(payload).await;
                        } else {
                            error!("Failed to parse TestResultPayload: {:?}", json_value);
                        }
                    }
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    dotenv().ok();

    let amqp_url = env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost".into());
    let backend_queue = env::var("BACKEND_QUEUE").unwrap_or_else(|_| "backend_core_queue".into());
    let test_results_queue =
        env::var("TEST_RESULTS_QUEUE").unwrap_or_else(|_| "test_results_queue".into());

    let conn = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    let channel_clone1 = channel.clone();
    let channel_clone2 = channel.clone();

    let backend_consumer = tokio::spawn(async move {
        consume_messages(&channel_clone1, &backend_queue, "backend_consumer").await
    });

    let test_results_consumer = tokio::spawn(async move {
        consume_messages(
            &channel_clone2,
            &test_results_queue,
            "test_results_consumer",
        )
        .await
    });

    // Wait for both consumers to complete (which they shouldn't under normal circumstances)
    let _ = tokio::try_join!(backend_consumer, test_results_consumer);

    Ok(())
}
