use crate::{error::Error, transaction::ParsedTransaction};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::client::db::ClientsDB;

pub struct ClientsManager {
    map: Arc<ClientsDB>,
}

impl ClientsManager {
    pub fn new(arc: Arc<ClientsDB>) -> ClientsManager {
        ClientsManager { map: arc }
    }

    pub async fn push_tx(&self, tx: ParsedTransaction) -> Result<(), Error> {
        let tx_id = tx.tx_id;
        let client_id = tx.client_id;

        // If there is a client in the DB, add it
        {
            let db_read = self.map.read().await;

            if db_read.contains_key(&client_id) {
                let mut client = db_read
                    .get(&client_id)
                    .ok_or(Error::FailedPushingTx)?
                    .write()
                    .await;
                client.add_transaction(tx_id, tx.extract_tx());

                return Ok(());
            }
        }

        {
            let mut db_write = self.map.write().await;

            // Check again, in case it got created right before we acquired the write_lock
            if db_write.contains_key(&client_id) {
                let mut client = db_write
                    .get(&client_id)
                    .ok_or(Error::FailedPushingTx)?
                    .write()
                    .await;
                client.add_transaction(tx_id, tx.extract_tx());
            } else {
                let (tx_details, mut client) = tx.extract_tx_and_client();
                client.add_transaction(tx_id, tx_details);

                db_write.insert(client_id, RwLock::new(client));
            }
        }
        Ok(())
    }
}
