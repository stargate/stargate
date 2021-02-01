package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.CassandraPersistenceActivator.makeConfig;

import io.stargate.db.Persistence;
import io.stargate.it.PersistenceTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
import org.assertj.core.util.VisibleForTesting;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class CassandraPersistenceIT extends PersistenceTest {

  private static CassandraPersistence persistence;
  private static File baseDir;
  @VisibleForTesting static Config config;

  @VisibleForTesting
  static CassandraPersistence initializePersistence(Config config) {
    CassandraPersistence cassandraPersistence = new CassandraPersistence();
    cassandraPersistence.initialize(config);
    return cassandraPersistence;
  }

  @VisibleForTesting
  static Config setUpConfig(ClusterConnectionInfo backend, File baseDirectory) throws IOException {

    System.setProperty("stargate.listen_address", "127.0.0.11");
    System.setProperty("stargate.cluster_name", backend.clusterName());
    System.setProperty("stargate.datacenter", backend.datacenter());
    System.setProperty("stargate.rack", backend.rack());
    ClassLoader classLoader = CassandraPersistenceIT.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }

    return makeConfig(baseDirectory);
  }

  @BeforeAll
  public static void createPersistence(ClusterConnectionInfo backend) throws IOException {
    baseDir = Files.createTempDirectory("stargate-cassandra-3.11-test").toFile();
    config = setUpConfig(backend, baseDir);
    persistence = initializePersistence(config);
  }

  @AfterAll
  public static void cleanup() throws IOException {
    // TODO: persistence.destroy() - note: it gets an NPE in NativeTransportService.destroy ATM
    //    if (persistence != null) {
    //      persistence.destroy();
    //    }

    FileUtils.deleteDirectory(baseDir);
  }

  @Override
  protected Persistence persistence() {
    return persistence;
  }
}
