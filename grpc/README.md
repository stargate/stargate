# gRPC Service

## gRPC Java Client 

### Setup

Let's see how to configure a java project to use the Stargate gRPC API.
The generated code based on protobuf files (`query.proto` and `stargate.proto`) is shipped with the `grpc-proto` dependency.

To see a guide how it compiles the Java code see: [gRPC setup project dependencies].
In your client application, you only need to add one dependency:

```xml
<dependencies>
    <dependency>
        <groupId>io.stargate.grpc</groupId>
        <artifactId>grpc-proto</artifactId>
        <version>1.0.32</version>
    </dependency>
</dependencies>
```
The last missing piece is to add a functional channel service provider. We pick netty:
```xml
<dependencies>
    <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-netty-shaded</artifactId>
            <version>1.40.1</version>
    </dependency>
</dependencies>
```
If you do not add it, you will observe the following error:
`No functional channel service provider found. Try adding a dependency on the grpc-okhttp, grpc-netty, or grpc-netty-shaded artifact`.

Once we have all needed dependencies we should be able to use the Stargate gRPC-stub API. 
After this step, you should have `StargateGrpc` available on your class path (from the `grpc-proto` dependency).
It contains the logic for interacting with Stargate gRPC API.

### Usage

Once we have the generated code, we are ready to create the client based on that.
Before delving into the code, we need to generate the auth token that will be used to perform Authorization, 
to see how to do it, please visit the [Stargate Authz documentation].
For the development purpose, this should be good enough:
```shell script
curl -X POST localhost:8081/v1/auth/token/generate --header "Content-Type: application/json" --data '{"key":"cassandra","secret":"cassandra"}'
```
assuming that Stargate is running on the `localhost:8081`  

Once we have that, we can connect to the gRPC API. Firstly, we need to generate the `Channel` that is used to perform connection:
```java
public ManagedChannel createChannel(String host, int port) {
    return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
}
```
Please note that `usePlaintext()` should only be used for development and testing.
When using in a production environment it should use a load balancer that terminates TLS.

For local development of stargate, it will be:
```java
ManagedChannel channel = createChannel("localhost", 8090);
```

Next, we can generate the `StargateGrpc` stub. There are two ways of interacting with the gRPC API.

The first one is synchronous (blocking):
```java
import io.stargate.grpc.StargateBearerToken;

StargateGrpc.StargateBlockingStub blockingStub = StargateGrpc.newBlockingStub(channel)
                                                    .withCallCredentials(new StargateBearerToken("token-value"))
                                                    .withDeadlineAfter(5, TimeUnit.SECONDS);
```

The second one is async (non-blocking):
```java
StargateGrpc.StargateStub = = StargateGrpc.newStub(channel)
                                .withCallCredentials(new StargateBearerToken("token-value"))
                                .withDeadlineAfter(5, TimeUnit.SECONDS);
```

Please note, that we need setup the `CallCredentials`, using the `token-value` generated in the previous step.
We also override the default [deadline](README.md#timeouts-deadlines).

We will assume that all queries are executed within the existing keyspace `ks` and table `test`.
The table definition is following:
```cql
CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))
``` 

The Stargate gRPC API provides a way to execute two types of queries:
- standard CQL queries
- batch queries (contains N CQL queries)

#### Standard Query

We can start from inserting a record using gRPC stub:

```java
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass;

Response response =
        blockingStub.executeQuery(
                        QueryOuterClass.Query
                                    .newBuilder().setCql("INSERT INTO ks.test (k, v) VALUES ('a', 1)").build());
```
This will build and execute a single query. 


Next, we can retrieve the inserted record(s):
```java
Response response = stub.executeQuery(QueryOuterClass.Query.newBuilder().setCql("SELECT k, v FROM ks.test").build());

```

If we print out the result set, it will have the following structure:
```yaml
result_set {
  data {
    type_url: "type.googleapis.com/stargate.ResultSet"
    value: "some_binary_data"
  }
}
```    
The value contains the binary data, that we can deserialize. 
Firstly, we need to unwrap the `ResultSet`:
```java
ResultSet rs = response.getResultSet().getData().unpack(QueryOuterClass.ResultSet.class);

rs.getRows(0).getValues(0).getString(); // it will return value for k = "a"
rs.getRows(0).getValues(1).getInt(); // it will return value for v = 1             
```
We can get all rows `getRowsList()` and iterate over the result 
OR get the specific row using its index and passing it to the `getRows(int index)` method. We picked the formed approach.
Our retrieval query (`SELECT k, v FROM test"`) stated explicitly which columns should be retrieved.
Thanks to that, we can safely get the values using their positions via the `getValues()` method.
The `getString()` and `getInt()` perform deserialization of data. The API provides utility methods for deserialization for more types.
For the full list of available types, see `Value` section in the [query.proto] file. 
 
#### Batch Query

In case we want to execute N queries, we can use the `executeBatch` method:
```java
     QueryOuterClass.Response response =
                blockingStub.executeBatch(
                        QueryOuterClass.Batch.newBuilder()
                                .addQueries(QueryOuterClass.BatchQuery.newBuilder().setCql("INSERT INTO ks.test (k, v) VALUES ('a', 1)").build())
                                .addQueries(
                                        QueryOuterClass.BatchQuery.newBuilder().setCql("INSERT INTO ks.test (k, v) VALUES ('b', 2)").build())
                                .build());

```

It takes the `Batch` as an argument. A Batch can contain N queries. We are adding two queries via `addQueries` method.

#### Async API

Up to this point, we were using the blocking version of the generated stub. 
We can also interact with the Stargate API using the async version of the stub.
To do so, we need to pass the [StreamObserver] that will be called asynchronously when the results are available.

Every StreamObserver needs to implement 3 methods: `onNext()`, `onError()` and `onComplete()`.
For example:
```java
StreamObserver<QueryOuterClass.Response> streamObserver = new StreamObserver<QueryOuterClass.Response>() {
           @Override
           public void onNext(QueryOuterClass.Response response) {
               try {
                   System.out.println("response:" + response.getResultSet().getData().unpack(QueryOuterClass.ResultSet.class));
               } catch (InvalidProtocolBufferException e) {
                   throw new RuntimeException(e);
               }
           }
           @Override
           public void onError(Throwable throwable) {
               System.out.println("Error: " + throwable);
           }
           @Override
           public void onCompleted() {
               System.out.println("completed");
           }
       };
```
Please note that this is a very simplified version only for demonstration purposes and should not be used on production.

Once we have the Observer, we can pass it to the `executeQuery` method on the async stub:
```java
stub.executeQuery(QueryOuterClass.Query.newBuilder().setCql("SELECT k, v FROM ks.test").build(), streamObserver);
```
This query will return immediately because it is non-blocking. 
If your program (or test) is progressing to the end, you may not be able to see the results. 
Your program may exist before the will arrives. 
After some time, when the data arrives, the `streamObserver` will be called.

The output of our program will look like this:
```yaml
response:columns {
  type {
    basic: VARCHAR
  }
  name: "k"
}
columns {
  type {
    basic: INT
  }
  name: "v"
}
rows {
  values {
    string: "a"
  }
  values {
    int: 1
  }
}

completed
```   
Please note, that at the end we have a `completed` emitted. This is called by the `onCompleted` method.

  

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

[deadline]: https://grpc.io/blog/deadlines/ 
[gRPC setup project dependencies]: https://github.com/grpc/grpc-java/blob/master/README.md#download
[Stargate Authz documentation]: https://stargate.io/docs/stargate/1.0/developers-guide/authnz.html
[query.proto]: ../grpc-proto/proto/query.proto
[StreamObserver]: https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html