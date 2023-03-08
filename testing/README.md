# Stargate Testing

This module provides integration tests for the Stragate Coordinator.

## Writing tests

### Test order

Running all tests in this suite is expensive.
Thus, in order to save execution time, we order the execution of the test based on the JUnit `@Order` annotation.

There is a simple rule of thumb here.
You need to add the explicit order in case:

* Your test is defining a custom `@ClusterSpec` annotation properties, you should add `@Order(TestOrder.FIRST)` to your test class.
* Your test is defining a custom `@StargateSpec` annotation properties, you should add `@Order(TestOrder.LAST)` to your test class.

## Running tests

When running tests you need to select the Storage implementation to test against.

#### Cassandra 3.11

```shell
./mvnw clean install -DskipTests
./mvnw -pl testing -P it-cassandra-3.11 verify -DskipUnitTests
```

#### Cassandra 4.0

```shell
./mvnw clean install -DskipTests
./mvnw -pl testing -P it-cassandra-4.0 verify -DskipUnitTests
```

#### DSE 6.8

```shell
./mvnw -P dse clean install -DskipTests
./mvnw -pl testing -P dse -P it-dse-6.8 verify -DskipUnitTests
```
