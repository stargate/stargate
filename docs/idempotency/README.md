# IdempotencyAnalyzer
Each of the Stargate persistence implementations (C* 3.11, C* 4.0 and DSE) are providing 
the IdempotencyAnalyzer component:
- [IdempotencyAnalyzer C* 3.11]
- [IdempotencyAnalyzer C* 4.0]
- [IdempotencyAnalyzer DSE]

Its responsibility is to analyze `CqlStatement`s and infer whether the query is idempotent or not.
All prepared queries are analyzed.
If yes, it is safe to retry such a query on the client or/and server-side. 
If it is not, the query should not be retried because it may result in an inconsistent state of a Database.

## Idempotency rules
The analyzer returns that query is non-idempotent in the following scenarios:
- List appends, prepends, or element deletions.
- Counter increments / decrements.
- Mutations that use non-idempotent functions such as `now()` and `uuid()`
- LWTs
- Truncate, schema changes, and USE

Batches are idempotent if their underlying statements are all idempotent. 
It is done by examining queries within a `BatchStatement`.   
It is worth noting that all reads are idempotent. 


[IdempotencyAnalyzer C* 3.11]: ../../persistence-cassandra-3.11/src/main/java/io/stargate/db/cassandra/impl/idempotency/IdempotencyAnalyzer.java
[IdempotencyAnalyzer C* 4.0]: ../../persistence-cassandra-4.0/src/main/java/io/stargate/db/cassandra/impl/idempotency/IdempotencyAnalyzer.java
[IdempotencyAnalyzer DSE]: ../../persistence-dse-6.8/src/main/java/io/stargate/db/dse/impl/idempotencyIdempotencyAnalyzer.java
