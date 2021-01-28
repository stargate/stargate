package io.stargate.db.cassandra.impl;

import java.util.Optional;

public class CassandraPersistenceWithConfigIT extends CassandraPersistenceIT {

  @Override
  protected Optional<String> getCassandraConfigPath() {
    return Optional.of("src/test/resources/cassandra.yaml");
  }

  @Override
  protected long getExpectedRowCacheSizeInMb() {
    return 1024;
  }
}
