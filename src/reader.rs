use std::sync::Arc;

use crate::{
    client::{
        queue::{push_tx, CQReceivers, CQSenders},
        ClientID,
    },
    error::Error,
    transaction::ParsedTransaction,
};
use async_channel::Sender;
use csv;
use tokio::sync::RwLock;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReadingStatusTypes {
    NotStarted,
    InProgress,
    Aborted,
    Done,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadingStatus {
    status: ReadingStatusTypes,
}

impl ReadingStatus {
    pub fn new() -> ReadingStatus {
        ReadingStatus {
            status: ReadingStatusTypes::NotStarted,
        }
    }

    pub fn change(&mut self, new_status: ReadingStatusTypes) {
        self.status = new_status;
    }

    pub fn get(&self) -> ReadingStatusTypes {
        self.status.clone()
    }
}

pub async fn start_reader(
    file_in: String,
    status: &Arc<RwLock<ReadingStatus>>,
    notification_sender: &Sender<ClientID>,
    senders: &Arc<CQSenders>,
    receivers: &Arc<CQReceivers>,
) {
    let reader_status = Arc::clone(&status);
    let notification_sender = notification_sender.clone();
    let senders = senders.clone();
    let receivers = receivers.clone();

    match read_file(
        file_in.as_str(),
        &reader_status,
        &notification_sender,
        &senders,
        &receivers,
    )
    .await
    {
        Ok(_) => reader_status.write().await.change(ReadingStatusTypes::Done),
        Err(_) => reader_status
            .write()
            .await
            .change(ReadingStatusTypes::Aborted),
    };
    debug!(
        "Finished reading with status: {:?}",
        reader_status.read().await.get()
    );
}

async fn read_file(
    file_in: &str,
    status: &Arc<RwLock<ReadingStatus>>,
    notification_sender: &Sender<ClientID>,
    senders: &Arc<CQSenders>,
    receivers: &Arc<CQReceivers>,
) -> Result<(), Error> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(file_in)
        .map_err(|_| Error::UnknownFile)?;

    status.write().await.change(ReadingStatusTypes::InProgress);

    let mut record = 0 as u64;
    for tx in reader.deserialize() {
        let tx: ParsedTransaction = match tx {
            Ok(tx) => tx,
            Err(e) => {
                debug!("E1 {:?}", e);
                continue;
            }
        };

        let client_id = tx.client_id;

        push_tx(senders, receivers, tx).await;

        match notification_sender.send(client_id).await {
            Ok(_) => (),
            Err(e) => {
                debug!("E2 {:?}", e);
                continue;
            }
        };
        record += 1;

        if record % 100_000 == 0 {
            debug!(
                "State: Parsed {} | On Queue {}",
                record,
                notification_sender.len()
            );
        }
    }

    Ok(())
}
