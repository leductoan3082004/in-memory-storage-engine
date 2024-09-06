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

### Some first assumptions

- Unlike normal databases when transactions are in the middle of running, if the databases are down, we must restore the data to originals. But in this engine, maybe we will not do that ? cause this is in memory storage engine, all memories will be lost if the system is down. So we can reduce the requirements and just need to consider the case MVCC when running for transactions.

### Some first approaches
- Key value store &rArr; use a map for this 
- Concurrency handling will need a RWLock (Golang already supported this), instead locking for all data, we just need to lock only which keys affected.
- For MVCC and repeated reads, I am considering this (maybe use a version id for this ? must be considered more).