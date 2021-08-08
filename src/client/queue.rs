use futures::future::poll_fn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use super::manager::ClientsManager;
use crate::transaction::ParsedTransaction;
use crate::{client::ClientID, error::Error};

pub type CQSenders = RwLock<HashMap<ClientID, Arc<RwLock<UnboundedSender<ParsedTransaction>>>>>;
pub type CQReceivers = RwLock<HashMap<ClientID, Arc<RwLock<UnboundedReceiver<ParsedTransaction>>>>>;

pub fn generate_clients_queues() -> (CQSenders, CQReceivers) {
    let senders = RwLock::new(HashMap::<
        ClientID,
        Arc<RwLock<UnboundedSender<ParsedTransaction>>>,
    >::new());
    let receivers = RwLock::new(HashMap::<
        ClientID,
        Arc<RwLock<UnboundedReceiver<ParsedTransaction>>>,
    >::new());

    (senders, receivers)
}

async fn push_without_adding(
    senders: &Arc<CQSenders>,
    tx: &ParsedTransaction,
) -> Result<bool, Error> {
    let client_id = tx.client_id;
    let readable_pile = senders.read().await;
    if readable_pile.contains_key(&client_id) {
        match readable_pile.get(&client_id) {
            Some(sender) => match sender.write().await.send(tx.clone()) {
                Ok(_) => Ok(true),
                Err(_) => Err(Error::FailedPushingTx),
            },
            None => Ok(false),
        }
    } else {
        Ok(false)
    }
}

pub async fn push_tx(
    senders: &Arc<CQSenders>,
    receivers: &Arc<CQReceivers>,
    tx: ParsedTransaction,
) {
    let client_id = tx.client_id;
    let added: bool = match push_without_adding(senders, &tx).await {
        Ok(result) => result,
        Err(_) => return, // ignore it
    };

    if !added {
        let (client_sender, client_receiver): (
            UnboundedSender<ParsedTransaction>,
            UnboundedReceiver<ParsedTransaction>,
        ) = unbounded_channel();

        {
            let mut s = senders.write().await;
            let mut r = loop {
                match receivers.try_write() {
                    Ok(v) => break v,
                    Err(_) => continue,
                };
            };

            s.insert(client_id, Arc::new(RwLock::new(client_sender)));
            r.insert(client_id, Arc::new(RwLock::new(client_receiver)));
        }

        push_without_adding(senders, &tx).await.ok();
    }
}

pub async fn consume(
    receivers: &Arc<CQReceivers>,
    client_id: ClientID,
    clients_manager: &ClientsManager,
) {
    // Get this client specific channel receiver
    let receiver = {
        let receivers = receivers.clone();

        let receivers = loop {
            match receivers.try_read() {
                Ok(v) => break v,
                Err(_) => continue,
            };
        };

        if receivers.contains_key(&client_id) {
            match receivers.get(&client_id) {
                Some(recv) => Some(recv.clone()),
                None => None,
            }
        } else {
            None
        }
    };

    match receiver {
        Some(receiver) => loop {
            let mut recv = receiver.write().await;

            match poll_fn(|cx| match recv.poll_recv(cx) {
                std::task::Poll::Ready(v) => std::task::Poll::Ready(v),
                std::task::Poll::Pending => std::task::Poll::Ready(None),
            })
            .await
            {
                Some(tx) => match clients_manager.push_tx(tx).await {
                    Ok(_) => (),
                    Err(_e) => {
                        // debug!("Error pushing parsed transaction: {}", _e);
                    }
                },
                None => {
                    break;
                }
            }
        },
        None => {}
    };
}
