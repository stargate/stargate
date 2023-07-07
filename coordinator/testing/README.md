# Stargate Testing

This module provides integration tests for the Stargate Coordinator.

## Integration Tests

### Writing Integration Tests

#### Test order

Running all tests in this suite is expensive.
Thus, in order to save execution time, we order the execution of the test based on the JUnit `@Order` annotation.

There is a simple rule of thumb here.
You need to add the explicit order in case:

* Your test is defining a custom `@ClusterSpec` annotation properties, you should add `@Order(TestOrder.FIRST)` to your test class.
* Your test is defining a custom `@StargateSpec` annotation properties, you should add `@Order(TestOrder.LAST)` to your test class.

### Running Integration Tests

When running tests you need to select the Storage implementation to test against.

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

-----

## Micro Benchmarks

This module also contains [JMH](https://github.com/openjdk/jmh) benchmarks for Stargate Coordinator.
It uses the [jmh-maven-plugin](https://github.com/metlos/jmh-maven-plugin) to run the benchmarks, primarily from the command line.

### Running benchmarks

> **NOTE:** It's advised that before running the benchmarks you build the project with `./mvnw clean install -DskipTests` to ensure that the latest changes are included.

To run all benchmarks, ensure you are in the `testing` directory and run:

```bash
../mvnw jmh:benchmark
```

To run a specific benchmark class:

```bash
../mvnw jmh:benchmark -Djmh.benchmarks=MyBenchmark
```

The JHM framework allows specification of different parameters for a benchmark run.
Check how the parameter passing works in the [jmh-maven-plugin](https://github.com/metlos/jmh-maven-plugin#passing-parameters) documentation.

### Using profilers

JMH comes with a set of available profilers that can be used to analyze the benchmark runs.
You can get the list of available profilers by running:

```bash
../mvnw jmh:benchmark -Djmh.lprof
```

To use a profiler, you need to specify it in the `-Djmh.prof=<profiler>` parameter.
For example, running with `gc` profiler:

```bash
../mvnw jmh:benchmark -Djmh.prof=gc
```

Very good tutorial on using profilers with JMH can be found [here](https://gist.github.com/markrmiller/a04f5c734fad879f688123bc312c21af#jmh-profilers).

### Using perf profilers

The `linux-tools-common` bring a set of useful `perf` profilers.
In order to properly setup the local environment, you need to do the following:

```bash
sudo apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r`
sudo sysctl -w kernel.perf_event_paranoid=-1
sudo sysctl --system
```

Listing the profilers should show new available profiles now.

### Using async-profiler

The support for [async-profiler](https://github.com/async-profiler/async-profiler) is also available.
You'll need to download the profiler and unpack it somewhere on your system.
Then, you can use one of the following methods to setup the library on your system:

> Ensure asyncProfiler library is on LD_LIBRARY_PATH (Linux), DYLD_LIBRARY_PATH (Mac OS), or -Djava.library.path. Alternatively, point to explicit library location with -prof async:libPath=<path>.

To pass options to the async-profiler, you can use the following specification:

```bash
../mvnw jmh:benchmark -Djmh.prof=async:libPath=[PATH_TO]/libasyncProfiler.so\;output=flamegraph\;dir=profile-results\;event=alloc
```

