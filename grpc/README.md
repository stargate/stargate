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