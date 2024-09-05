# In memory storage engine


### Requirements
- Type of storage: key value store (redis like ?).
- Concurrent reads do not block each others.
- Writes do not block reads.
- But write block write (each write happen serialize).
- #### Transactions
  - For transaction, must support repeated reads.
  - Must support MVCC (Multi version concurrency control).


- Support indexing (like database) for quick queries or writes.
- Must have UI / UX to interact with engine (maybe simple).
- How to test that this engine run correctly or not ?
- Code structure.
- Documentation.
