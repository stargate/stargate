# gRPC Service

## Timeouts (Deadlines)
The gRPC client (generated stub) [deadline] should be set for all your clients.
Depending on the language, the default value may be infinite or very high. 
For Java, it is set to 10 seconds by default.
The deadline can be set using the `withDeadlineAfter` method:
```java
blockingStub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
```

For the Stargate gRPC client, we recommend setting it to at most 5 seconds.
The reason for it is the fact that Stargate sets the Cassandra timeout to 5 seconds:
```yaml
read_request_timeout_in_ms: 5000 
```
If the client sets the deadline to > 5 seconds, there will be a situation when a request times out on the Stargate server-side, but the client will still wait for it.

However, if you are executing DDL queries (e.g.: `"CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};"` or `CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))`)
you may need to increase the deadline above 5 seconds.

[deadline]: https://grpc.io/blog/deadlines/