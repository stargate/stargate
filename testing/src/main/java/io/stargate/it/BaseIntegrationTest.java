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
package io.stargate.it;

import com.datastax.oss.driver.api.core.Version;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.StargateSpec;
import io.stargate.it.storage.UseStargateCoordinator;
import java.io.File;
import java.net.URL;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;

/** This class manages starting Stargate coordinator nodes. */
@UseStargateCoordinator
@ClusterSpec(shared = true)
@StargateSpec(shared = true)
public class BaseIntegrationTest {

  protected ClusterConnectionInfo backend;

  static {
    ClassLoader classLoader = BaseIntegrationTest.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }
  }

  @BeforeEach
  public void init(ClusterConnectionInfo backend) {
    this.backend = backend;
  }

  // TODO generalize this to an ExecutionCondition that reads custom annotations, like
  // @CassandraRequirement/@DseRequirement in the Java driver tests
  public boolean isCassandra4() {
    return !backend.isDse()
        && Version.parse(backend.clusterVersion()).nextStable().compareTo(Version.V4_0_0) >= 0;
  }

  public boolean backendSupportsSAI() {
    return backend.supportsSAI();
  }

  public static Instant now() {
    // Avoid using Instants with nanosecond precision as nanos may be lost on the server side
    return Instant.ofEpochMilli(System.currentTimeMillis());
  }
}
