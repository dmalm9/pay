use crate::client::{Client, ClientID};
use std::collections::HashMap;
use tokio::sync::RwLock;

// When updating client data, we only want to read_lock the hashmap
//    and write_lock the client
// When adding a client, we want to write_lock the hashmap
//    and add the client
pub type ClientsDB = RwLock<HashMap<ClientID, RwLock<Client>>>;

pub fn generate_client_db() -> ClientsDB {
    RwLock::new(HashMap::<ClientID, RwLock<Client>>::new())
}
