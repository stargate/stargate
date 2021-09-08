# gRPC Service

## gRPC Java Client 

Let's see how to configure a java project to use the Stargate gRPC API.
We will use the protobuf files (`query.proto` and `stargate.proto`) to generate the gRPC client stub.

Firstly, you need to add the protobuf files to your project. 
You can copy-paste both of them or add a `stargate-grpc-proto` module as a git submodule to your repo:   
`git submodule add https://github.com/stargate/stargate-grpc-proto`

It will copy all necessary proto-files to your repository.
Once we have those files, we need to add all the needed dependencies to your project.
To see how to achieve that, see: [gRPC setup project dependencies].
If you are using maven, please add the following dependencies to your `pom.xml`:
```xml
<dependencies>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-netty-shaded</artifactId>
      <version>1.40.1</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-protobuf</artifactId>
      <version>1.40.1</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-stub</artifactId>
      <version>1.40.1</version>
    </dependency>
    <dependency> <!-- necessary for Java 9+ -->
      <groupId>org.apache.tomcat</groupId>
      <artifactId>annotations-api</artifactId>
      <version>6.0.53</version>
      <scope>provided</scope>
    </dependency>
</dependencies>
```

Besides those, you need to add:
```xml
<dependencies>
    <dependency>
       <groupId>io.grpc</groupId>
       <artifactId>grpc-core</artifactId>
       <version>1.40.1</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-api</artifactId>
        <version>1.40.1</version>
    </dependency>
</dependencies>
```
We need those dependencies to be able to use the gRPC-stub API. 

Finally, we need a way to generate the java classes based on the protobuf files.
We will use the `protobuf-maven-plugin` for this purpose:

```xml
<build>
  <extensions>
    <extension>
      <groupId>kr.motd.maven</groupId>
      <artifactId>os-maven-plugin</artifactId>
      <version>1.6.2</version>
    </extension>
  </extensions>
  <plugins>
    <plugin>
      <groupId>org.xolstice.maven.plugins</groupId>
      <artifactId>protobuf-maven-plugin</artifactId>
      <version>0.6.1</version>
      <configuration>
        <protocArtifact>com.google.protobuf:protoc:3.17.3:exe:${os.detected.classifier}</protocArtifact>
        <pluginId>grpc-java</pluginId>
        <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.40.1:exe:${os.detected.classifier}</pluginArtifact>
        <protoSourceRoot>stargate-grpc-proto</protoSourceRoot>
      </configuration>
      <executions>
        <execution>
          <goals>
            <goal>compile</goal>
            <goal>compile-custom</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

Please note that we are configuring `<protoSourceRoot>` to point to `stargate-grpc-proto`.
This is a directory added via git submodule to your project. 
In case you use a different mechanism (copy-paste) to import the protobuf files to your project,
you need to adapt this `<protoSourceRoot>` accordingly.

Finally, we can compile our code:
`mvn clean compile`
After this step, you should have `StargateGrpc` class generated in your target directory.
It contains the logic for interacting with Stargate gRPC API. This logic is generated based on the protobuf files.


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