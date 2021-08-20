use crate::error::Error;
use crate::transaction::{
    Transaction, TransactionID, TransactionType, TransactionsMap, UniqueTransactionIDs,
};
use rust_decimal::Decimal;
use serde::{Serialize, Serializer};

pub type ClientID = u16;

#[derive(Debug)]
pub struct Client {
    pub id: ClientID,
    pub available: Decimal,
    pub held: Decimal,
    pub locked: bool,

    deposits: TransactionsMap,
    disputed_deposit_ids: UniqueTransactionIDs,
    seen_transaction_ids: UniqueTransactionIDs,
}

impl Client {
    pub fn new(id: ClientID) -> Client {
        Client {
            id,
            available: Decimal::from(0),
            held: Decimal::from(0),
            locked: false,
            deposits: TransactionsMap::new(),
            disputed_deposit_ids: UniqueTransactionIDs::new(),
            seen_transaction_ids: UniqueTransactionIDs::new(),
        }
    }

    pub fn add_transaction(&mut self, txid: TransactionID, tx: Transaction) {
        if !self.locked {
            match self.process_transaction(txid, tx) {
                Ok(_) => (),
                Err(_e) => {
                    // debug!("Transaction [{}]: {} ", txid, _e)
                }
            };
        }
    }

    fn get_tx_amount(&self, txid: TransactionID) -> Result<Decimal, Error> {
        self.deposits
            .get(&txid)
            .ok_or(Error::TransactionAccessError)?
            .get_amount()
    }

    fn process_transaction(&mut self, txid: TransactionID, tx: Transaction) -> Result<(), Error> {
        match tx.tx_type {
            TransactionType::Deposit => self.deposit(txid, tx),
            TransactionType::Withdrawal => self.withdrawal(txid, tx),
            TransactionType::Dispute => self.dispute(txid),
            TransactionType::Resolve => self.resolve(txid),
            TransactionType::Chargeback => self.chargeback(txid),
        }
    }
    fn deposit(&mut self, txid: TransactionID, tx: Transaction) -> Result<(), Error> {
        if self.seen_transaction_ids.contains(&txid) {
            Err(Error::DuplicateTransaction)
        } else if tx.amount.is_none() {
            Err(Error::InvalidAmount)
        } else {
            self.seen_transaction_ids.insert(txid);

            let amount = tx.get_amount()?;

            self.available += amount;
            self.deposits.insert(txid, tx);
            Ok(())
        }
    }

    fn withdrawal(&mut self, txid: TransactionID, tx: Transaction) -> Result<(), Error> {
        if self.seen_transaction_ids.contains(&txid) {
            Err(Error::DuplicateTransaction)
        } else if tx.amount.is_none() {
            Err(Error::InvalidAmount)
        } else {
            self.seen_transaction_ids.insert(txid);

            let amount = tx.get_amount()?;

            if self.available >= amount {
                self.available -= amount;
                Ok(())
            } else {
                Err(Error::NoAvailableFunds)
            }
        }
    }

    fn dispute(&mut self, txid: TransactionID) -> Result<(), Error> {
        if !self.deposits.contains_key(&txid) {
            Err(Error::TransactionNotFound)
        } else if self.disputed_deposit_ids.contains(&txid) {
            Err(Error::DuplicateDispute)
        } else {
            let disputed_amount = self.get_tx_amount(txid)?;

            let result = if self.available >= disputed_amount {
                self.available -= disputed_amount;
                self.held += disputed_amount;
                self.disputed_deposit_ids.insert(txid);

                Ok(())
            } else {
                Err(Error::NoAvailableFunds)
            };

            result
        }
    }

    fn resolve(&mut self, txid: TransactionID) -> Result<(), Error> {
        if !self.deposits.contains_key(&txid) {
            Err(Error::TransactionNotFound)
        } else if !self.disputed_deposit_ids.contains(&txid) {
            Err(Error::DisputeNotFound)
        } else {
            let disputed_amount = self.get_tx_amount(txid)?;

            let result = if self.held >= disputed_amount {
                self.available += disputed_amount;
                self.held -= disputed_amount;
                self.disputed_deposit_ids.remove(&txid);

                Ok(())
            } else {
                Err(Error::MissingHeldFunds)
            };

            result
        }
    }

    fn chargeback(&mut self, txid: TransactionID) -> Result<(), Error> {
        if !self.deposits.contains_key(&txid) {
            Err(Error::TransactionNotFound)
        } else if !self.disputed_deposit_ids.contains(&txid) {
            Err(Error::DisputeNotFound)
        } else {
            let disputed_amount = self.get_tx_amount(txid)?;

            let result = if self.held >= disputed_amount {
                self.held -= disputed_amount;
                self.disputed_deposit_ids.remove(&txid);
                self.locked = true;

                Ok(())
            } else {
                Err(Error::NotEnoughChargeback)
            };

            result
        }
    }
}

#[derive(Serialize)]
pub struct SerializableClient {
    client: ClientID,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl Serialize for Client {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializableClient {
            client: self.id,
            available: self.available.round_dp(4),
            held: self.held.round_dp(4),
            total: (self.held + self.available).round_dp(4),
            locked: self.locked,
        }
        .serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;
    use std::{panic, str::FromStr};

    const DEPOSIT_AMOUNT: u32 = 100;
    const INIT_DEPOSIT_COUNT: u32 = 3;

    fn init() -> Client {
        {
            panic::set_hook(Box::new(|_info| {}));

            panic::catch_unwind(|| {
                // It can only be run once.
                env_logger::init();
            })
            .unwrap_or(());
        }

        let mut client = Client::new(1);

        for txid in 0..INIT_DEPOSIT_COUNT {
            client.add_transaction(
                txid,
                Transaction {
                    tx_type: TransactionType::Deposit,
                    amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
                },
            );
        }

        assert_eq!(
            Decimal::from(DEPOSIT_AMOUNT * INIT_DEPOSIT_COUNT),
            client.available
        );
        assert_eq!(Decimal::from(0), client.held);
        assert_eq!(false, client.locked);

        client
    }

    fn test_ignored(client: &mut Client, txid: TransactionID, tx: Transaction) {
        let prev_available = client.available;
        let prev_held = client.held;

        client.add_transaction(txid, tx);

        assert_eq!(prev_available, client.available);
        assert_eq!(prev_held, client.held);
    }

    fn test_success_dispute(client: &mut Client, txid: TransactionID, tx: Transaction) {
        let prev_available = client.available;
        let prev_held = client.held;

        client.add_transaction(txid, tx);

        assert_eq!(client.held, prev_held + Decimal::from(DEPOSIT_AMOUNT));
        assert_eq!(
            client.available,
            prev_available - Decimal::from(DEPOSIT_AMOUNT)
        );
    }

    fn test_success_resolve(client: &mut Client, txid: TransactionID, tx: Transaction) {
        let prev_available = client.available;
        let prev_held = client.held;

        client.add_transaction(txid, tx);

        assert_eq!(client.held, prev_held - Decimal::from(DEPOSIT_AMOUNT));
        assert_eq!(
            client.available,
            prev_available + Decimal::from(DEPOSIT_AMOUNT)
        );
    }

    #[tokio::test]
    async fn test_duplicate_id() {
        let mut client = init();

        let initial_amount = client.available;

        client.add_transaction(
            1,
            Transaction {
                tx_type: TransactionType::Deposit,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );

        assert_eq!(initial_amount, client.available);

        client.add_transaction(
            1,
            Transaction {
                tx_type: TransactionType::Withdrawal,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );
        assert_eq!(initial_amount, client.available);
    }

    #[tokio::test]
    async fn test_disputes_and_resolves() {
        let mut client = init();

        assert!(client.available > Decimal::from(0));
        assert_eq!(client.held, Decimal::from(0));

        let dispute_tx = Transaction {
            tx_type: TransactionType::Dispute,
            amount: None,
        };

        let resolve_tx = Transaction {
            tx_type: TransactionType::Resolve,
            amount: None,
        };

        let valid_dispute_id = 1;

        // Test dispute tx ID & passing the amount field, which should be ignored
        let mut dispute_with_amount = dispute_tx.clone();
        dispute_with_amount.amount = Some(Decimal::from(DEPOSIT_AMOUNT + 1000));
        test_success_dispute(&mut client, valid_dispute_id, dispute_with_amount);

        // Test duplicate dispute
        test_ignored(&mut client, valid_dispute_id, dispute_tx.clone());

        // Test disputing unexistent tx
        test_ignored(&mut client, 100, dispute_tx.clone());

        // Test resolving unexistent dispute
        test_ignored(&mut client, 101, resolve_tx.clone());

        // Test withdrawling all funds when on dispute
        let total = client.available + client.held;
        test_ignored(
            &mut client,
            INIT_DEPOSIT_COUNT + 1,
            Transaction {
                tx_type: TransactionType::Withdrawal,
                amount: Some(total),
            },
        );

        // Test resolving
        test_success_resolve(&mut client, valid_dispute_id, resolve_tx.clone());
        assert_eq!(client.held, Decimal::from(0));

        // Test resolving again
        test_ignored(&mut client, valid_dispute_id, resolve_tx.clone());

        // Test disputing same ID after resolving it
        test_success_dispute(&mut client, valid_dispute_id, dispute_tx);

        // Test resolving
        test_success_resolve(&mut client, valid_dispute_id, resolve_tx.clone());
        assert_eq!(client.held, Decimal::from(0));

        // Test withdrawling all funds after resolving
        let prev_available = client.available;
        let prev_held = client.held;

        client.add_transaction(
            INIT_DEPOSIT_COUNT + 2,
            Transaction {
                tx_type: TransactionType::Withdrawal,
                amount: Some(prev_available + prev_held),
            },
        );
        assert_eq!(client.available, Decimal::from(0));
        assert_eq!(client.held, Decimal::from(0));
    }

    #[tokio::test]
    async fn test_chargeback() {
        let mut client = init();
        let dispute_tx = Transaction {
            tx_type: TransactionType::Dispute,
            amount: None,
        };
        let chargeback_tx = Transaction {
            tx_type: TransactionType::Chargeback,
            amount: None,
        };
        let dispute_id = 1;

        test_success_dispute(&mut client, dispute_id, dispute_tx);

        // Test chargeback non existent id
        test_ignored(&mut client, 100, chargeback_tx.clone());

        // Test chargeback
        let prev_available = client.available;
        let prev_held = client.held;

        client.add_transaction(dispute_id, chargeback_tx.clone());

        assert_eq!(client.held, prev_held - Decimal::from(DEPOSIT_AMOUNT));
        assert_eq!(client.available, prev_available);

        // Test chargeback same id [ it's locked, so it should be ignored]
        test_ignored(&mut client, dispute_id, chargeback_tx.clone());

        // Test all other transation types [ it's locked, so they should all be ignored]
        test_ignored(
            &mut client,
            100,
            Transaction {
                tx_type: TransactionType::Deposit,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );

        test_ignored(
            &mut client,
            101,
            Transaction {
                tx_type: TransactionType::Withdrawal,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );

        test_ignored(
            &mut client,
            1,
            Transaction {
                tx_type: TransactionType::Dispute,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );

        test_ignored(
            &mut client,
            1,
            Transaction {
                tx_type: TransactionType::Resolve,
                amount: Some(Decimal::from(DEPOSIT_AMOUNT)),
            },
        );
    }

    #[tokio::test]
    async fn test_serialize() {
        let client = Client {
            available: Decimal::from_str("2.1234220").unwrap(),
            held: Decimal::from_str("2.00006").unwrap(),
            locked: true,
            id: 1,
            deposits: TransactionsMap::new(),
            disputed_deposit_ids: UniqueTransactionIDs::new(),
            seen_transaction_ids: UniqueTransactionIDs::new(),
        };
        let compare_data = "client,available,held,total,locked\n1,2.1234,2.0001,4.1235,true\n";

        let mut writer = csv::Writer::from_writer(Vec::new());
        writer.serialize(client).unwrap();

        assert_eq!(
            String::from_utf8(writer.into_inner().unwrap()).unwrap(),
            compare_data
        );
    }
}
