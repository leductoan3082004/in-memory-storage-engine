# In memory storage engine


### Requirements
- Type of storage: key value store (redis like ?).
- Concurrent reads do not block each others.
- Writes do not block reads.
- But write does block write (each write happens serialize).
- #### Transactions
  - For transaction, must support repeated reads.
  - Must support MVCC (Multi version concurrency control).


- Support indexing (like database) for quick queries or writes. (consider for composite index if possible)
- Must have UI / UX to interact with engine (maybe simple).
- How to test that this engine runs correctly or not ?
- Code structure.
- Documentation.
- Absolutely consistency
- High availability ?


### Main functions
- Get (key): value
- Set (key)
- Delete(key)
- Transaction 
  - Begin
  - Commit
  - Rollback

### Some first assumptions

- Unlike normal databases when transactions are in the middle of running, if the databases are down, we must restore the data to originals. But in this engine, maybe we will not do that ? cause this is in memory storage engine, all memories will be lost if the system is down. So we can reduce the requirements and just need to consider the case MVCC when running for transactions.

### Some first approaches
- Key value store &rArr; use a map for this 
- Concurrency handling will need a RWLock (Golang already supported this), instead locking for all data, we just need to lock only which keys affected.

- #### For MVCC and repeated reads, the transaction will follow these steps:
  - Each key is assigned with a version number (this number will increase over time to track the latest version of the key). 
  - Assume the transaction will need to query and update key1, key2, key3, ..., we will create a snapshot of these key to use only in the transaction.
  - Then we will use these values to execute the transaction, so other transactions will still see the committed values
  - At the time we need to commit, just gain the lock for commit (for this solution I think we can just use one lock for all keys because of deadlock). So we can guarantee that only one transaction commit at a time, but how to know that this transaction is valid ? we just need to compare the version number of the keys in the snapshot and keys that have been committed, if the version matched then the transaction is valid, else the data that this transaction read before is stale (this mean there is another transaction that have committed before).
  - So users may feel the UX is bad if there are multiple transactions happen at same time (because of the errors).
  - We need to add a mechanism to automatically retry the transaction (if it is stale). Otherwise, if this transaction fail because of some condition checking, we just need to fail it.
    
