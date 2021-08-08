# Pay

### Run
```
cargo run --release -- fixtures/test.csv > result.csv 
```

### Environment Variables
* `RUST_LOG=debug`: for helpful messages/tracking.
* `NUM_WORKERS=3`: increase/decrease for performance tweaking.

---

## Considerations
* When a client is locked, it no longer accepts any other type of transaction
* Transaction errors (eg. wrong ids, duplicates) are logged (if active). The transaction will be skipped.

## General Overview
* `reader` deserializes transactions and pushes them to a shared  `ClientsQueues: Hashmap<ClientID, ClientQueue>`.
* `reader` notifies all `processors` of new transactions, by pushing the `ClientID` into a notification queue.
* At the same time, a number of `processors` will consume the `notification queue` to know which client they have to process.
* Each `processor` consumes a client queue from `ClientsQueues` and populates the `ClientsDB: Hashmap<ClientID, Client>` by processing the different transactions.
* Once all has been deserialized and processed, `writer` goes through all clients in `ClientsDB`, and outputs the final csv to stdout.


## Testing
```
cargo test
```
* `src/client.rs`: has tests that process different transaction types, and weird scenarios for a client. Also tests serialization.
* `src/transaction.rs`: has tests for deserializing transactions. (eg. spaces, invalid fields)
* `fixtures/test.csv`: simple sample data with one client. Tests the different transaction types using the whole program. Should result in:
```
id,available,held,total,locked
1,11.02,0.0000,11.02,true
```
* `fixtures/gen.py`: simple python script to generate big [random] data, to test loading the program.