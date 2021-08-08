use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidAmount,

    TransactionNotFound,
    DuplicateTransaction,

    TransactionAccessError,

    NoAvailableFunds,
    DuplicateDispute,
    DisputeNotFound,
    MissingHeldFunds,

    NotEnoughChargeback,

    UnknownFile,

    FailedPushingTx,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(Error: {:#?})", self)
    }
}
