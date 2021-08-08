use std::sync::Arc;

use async_channel::Receiver;
use futures_lite::future;
use tokio::sync::RwLock;

use crate::{
    client::{
        db::ClientsDB,
        manager::ClientsManager,
        queue::{consume, CQReceivers},
        ClientID,
    },
    reader::{ReadingStatus, ReadingStatusTypes},
};

pub async fn start_processors(
    max_processors: u32,
    reading_status: &Arc<RwLock<ReadingStatus>>,
    notification_receiver: &Receiver<ClientID>,
    clients: &Arc<ClientsDB>,
    receivers: &Arc<CQReceivers>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut threads = vec![];
    for _ in 0..max_processors {
        let status = Arc::clone(reading_status);
        let notifier = notification_receiver.clone();
        let client_db = Arc::clone(clients);
        let pile_receivers = Arc::clone(receivers);

        threads.push(tokio::spawn(async move {
            let clients_manager = ClientsManager::new(client_db);
            loop {
                {
                    match status.read().await.get() {
                        ReadingStatusTypes::Aborted | ReadingStatusTypes::Done => {
                            if notifier.is_empty() {
                                break;
                            }
                        }
                        _ => (),
                    };
                }

                let client_id = match future::block_on(notifier.recv()) {
                    Ok(client_id) => client_id,
                    Err(_) => {
                        continue;
                    }
                };

                consume(&pile_receivers, client_id, &clients_manager).await;
            }
        }));
    }

    threads
}
