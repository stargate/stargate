package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.Cassandra311PersistenceActivator.makeConfig;

import io.stargate.db.Persistence;
import io.stargate.it.PersistenceTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
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
class Cassandra311PersistenceIT extends PersistenceTest {

  private static Cassandra311Persistence persistence;
  private static File baseDir;

  @BeforeAll
  public static void createPersistence(ClusterConnectionInfo backend) throws IOException {
    baseDir = Files.createTempDirectory("stargate-cassandra-3.11-test").toFile();
    baseDir.deleteOnExit();

    System.setProperty("stargate.listen_address", "127.0.0.11");
    System.setProperty("stargate.cluster_name", backend.clusterName());
    System.setProperty("stargate.datacenter", backend.datacenter());
    System.setProperty("stargate.rack", backend.rack());
    ClassLoader classLoader = Cassandra311PersistenceIT.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }

    persistence = new Cassandra311Persistence();
    persistence.initialize(makeConfig(baseDir));
  }

  @AfterAll
  public static void cleanup() throws IOException {
    // TODO: persistence.destroy() - note: it gets an NPE in NativeTransportService.destroy ATM
    //    if (persistence != null) {
    //      persistence.destroy();
    //    }
  }

  @Override
  protected Persistence persistence() {
    return persistence;
  }
}
