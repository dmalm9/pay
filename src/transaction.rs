use crate::client::{Client, ClientID};
use crate::error::Error;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;

pub type TransactionID = u32;
pub type TransactionsMap = BTreeMap<TransactionID, Transaction>;
pub type UniqueTransactionIDs = HashSet<TransactionID>;

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,

    #[serde(rename = "withdrawal")]
    Withdrawal,

    #[serde(rename = "dispute")]
    Dispute,

    #[serde(rename = "resolve")]
    Resolve,

    #[serde(rename = "chargeback")]
    Chargeback,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedTransaction {
    #[serde(rename = "type")]
    pub tx_type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: ClientID,
    #[serde(rename = "tx")]
    pub tx_id: u32,
    #[serde(deserialize_with = "to_four_dp")]
    pub amount: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub tx_type: TransactionType,
    pub amount: Option<Decimal>,
}

impl ParsedTransaction {
    pub fn extract_tx(self) -> Transaction {
        Transaction {
            tx_type: self.tx_type,
            amount: self.amount,
        }
    }

    pub fn extract_tx_and_client(self) -> (Transaction, Client) {
        (
            Transaction {
                tx_type: self.tx_type,
                amount: self.amount,
            },
            Client::new(self.client_id),
        )
    }
}

impl Transaction {
    pub fn get_amount(&self) -> Result<Decimal, Error> {
        self.amount.ok_or(Error::InvalidAmount)
    }
}

fn to_four_dp<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match Decimal::from_str(s) {
        Ok(val) => Ok(Some(val.round_dp(4))),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv;

    #[tokio::test]
    async fn test_deserialize() {
        let tx = "type,client   ,       tx , amount     
        deposit     , 30283  ,      3032270210,37.44444444
        deposdsidt,2,1,a.44444444   
        chargeback,30283,3032270210,    
        deposiat,2,1,a.44444444
        resolve,30283,3032270210,
        depositsad,2,1,a.44444444
        dispute,30283,3032270210,37.44444444
        deposiasdt,2,1,a.44444444
        withdrawal,30283,3032270210,37.44444444
        deposdit,d2,1d,a.44444444
        depadfasdfosit,30283,3032270210,37.44444444
        withdrawal,adfa,3032270210,37.44444444
        deposit                     ,2,asd,0.44444444
        deposit,2,1,            a.44444444"
            .as_bytes();

        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(tx);

        let valid_transactions = &[
            ParsedTransaction {
                amount: Some(Decimal::from_str("37.4444").unwrap()),
                client_id: 30283,
                tx_id: 3032270210,
                tx_type: TransactionType::Deposit,
            },
            ParsedTransaction {
                amount: None,
                client_id: 30283,
                tx_id: 3032270210,
                tx_type: TransactionType::Chargeback,
            },
            ParsedTransaction {
                amount: None,
                client_id: 30283,
                tx_id: 3032270210,
                tx_type: TransactionType::Resolve,
            },
            ParsedTransaction {
                amount: Some(Decimal::from_str("37.4444").unwrap()),
                client_id: 30283,
                tx_id: 3032270210,
                tx_type: TransactionType::Dispute,
            },
            ParsedTransaction {
                amount: Some(Decimal::from_str("37.4444").unwrap()),
                client_id: 30283,
                tx_id: 3032270210,
                tx_type: TransactionType::Withdrawal,
            },
            ParsedTransaction {
                amount: None,
                client_id: 2,
                tx_id: 1,
                tx_type: TransactionType::Deposit,
            },
        ];

        let mut iter_der = reader.deserialize();
        let tx: Option<Result<ParsedTransaction, csv::Error>> = iter_der.next();

        assert_eq!(valid_transactions[0], tx.unwrap().unwrap());
        assert!(iter_der.next().unwrap().is_err());
        assert_eq!(valid_transactions[1], iter_der.next().unwrap().unwrap());
        assert!(iter_der.next().unwrap().is_err());
        assert_eq!(valid_transactions[2], iter_der.next().unwrap().unwrap());
        assert!(iter_der.next().unwrap().is_err());
        assert_eq!(valid_transactions[3], iter_der.next().unwrap().unwrap());
        assert!(iter_der.next().unwrap().is_err());
        assert_eq!(valid_transactions[4], iter_der.next().unwrap().unwrap());
        assert!(iter_der.next().unwrap().is_err());
        assert!(iter_der.next().unwrap().is_err());
        assert!(iter_der.next().unwrap().is_err());
        assert!(iter_der.next().unwrap().is_err());
        assert_eq!(valid_transactions[5], iter_der.next().unwrap().unwrap());
    }
}
