#[macro_use]
extern crate log;

mod client;
mod error;
mod processor;
mod reader;
mod transaction;
mod writer;

use client::db::generate_client_db;
use client::queue::{generate_clients_queues, CQReceivers, CQSenders};
use client::ClientID;

use processor::start_processors;
use reader::{start_reader, ReadingStatus};

use std::sync::Arc;
use tokio::sync::RwLock;
use writer::write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let file_in = args.get(1).unwrap();

    let reading_status = Arc::new(RwLock::new(ReadingStatus::new()));
    let client_db = Arc::new(generate_client_db());

    let (notifier_sender, notifier_receiver): (
        async_channel::Sender<ClientID>,
        async_channel::Receiver<ClientID>,
    ) = async_channel::unbounded();

    let (senders, receivers): (CQSenders, CQReceivers) = generate_clients_queues();
    let pile_senders = Arc::new(senders);
    let pile_receivers = Arc::new(receivers);

    let num_processors = std::env::var("NUM_WORKERS")
        .unwrap_or("3".to_string())
        .parse::<u32>()
        .unwrap_or(3);

    let processors = start_processors(
        num_processors,
        &reading_status,
        &notifier_receiver,
        &client_db,
        &pile_receivers,
    )
    .await;

    start_reader(
        file_in.clone(),
        &reading_status,
        &notifier_sender,
        &pile_senders,
        &pile_receivers,
    )
    .await;

    notifier_sender.close();
    futures::future::join_all(processors).await;

    write(client_db).await;

    Ok(())
}
