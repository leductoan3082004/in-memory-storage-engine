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
- High availability ? maybe not cuz this is in mem storage


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


### Some very first solutions
- Because we need the transaction to be repeatable read, that's mean we need to maintain the **committed** version of key (meaning one key will have multiple versions).
- Everytime we need to **READ** a value from a key, just return the latest version of that key that is visible (meaning the latest value that has been committed so far).
- When we perform a **SET OPERATION** we just treat it as a transaction with one operation, we will create a new version that contain the value and add to the version array of that key.
- The same for **DELETE OPERATION**, but now we do not actually delete the key out of our map, we actually create a version and mark that version as invisible so that everytime a read perform operation on it, it will return nothing due to the invisibility.
- Now the important part is transaction, since we are maintaining multiple versions of one key, so when we need to **START TRANSACTION**, we will generate a *transaction id* for it (actually this *transaction id* is a number that is increase by one overtime, and we just use it as a version number for easy management), and we will use this *transaction id* for communicate within this transaction only.
- Everytime we want to communicate with this transaction or make some changes relate to it, just need to send the operation with the *transaction id* along to identify.
- And everytime when we access a key in transaction we will generate a snapshot for that key and store in an isolated map in that transaction. 
  - We want to set the key **A** to value **ValueA**. We will need to consider multiple case:
    - If key **A** does not exist in the storage yet, so we simply create this key in the isolated data of transaction.
    - If key **A** exists before the transaction begins (how to track ? just need to iterate through the committed versions key **A** to know which is the latest value before this transaction begins). Then we just need to copy the value of **A** and then set it to the value we need.
    - If key **A** exists but invisible (this means has been deleted). Simply refer to case 1.
  
  - We want to delete key **A** ?
    - We will first check if key **A** exists in our isolated snapshot first, if yes, just mark it invisible, else we will just create the key with null value and mark it invisible.
    - If key **A** does not exist in our isolated snapshot yet, we will check if it exists in storage. If yes just create a key with null value then mark it invisible, else we can ignore it or throw an error for users.
- Now come to the **ABORT**, we just need to remove the transaction along with its snapshot.
- With **COMMIT**, we will iterate through the key, value snapshot and apply the changes to the main storage. We need to check if the latest version of each key is smaller than the *transaction id* (because we are using *transaction id* for versioning). If one of the keys has the latest version greater than current *transaction id*, this means we can not commit this transaction, because there is another transaction that has committed before that lead to the transaction number increases. At this time we can throw the error to user, and user may make the transaction from the beginning. But there is another approach, we will store all the operations of one transaction and retry it several times before forcing user make it again.