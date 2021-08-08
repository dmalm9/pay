use crate::client::db::ClientsDB;
use std::{io, sync::Arc};

pub async fn write(db: Arc<ClientsDB>) {
    let mut writer = csv::Writer::from_writer(io::stdout());

    let db_read = db.read().await;
    let iter_clients = db_read.values();

    for c in iter_clients {
        let client = c.read().await;
        writer.serialize(&*client).ok();
    }
}
