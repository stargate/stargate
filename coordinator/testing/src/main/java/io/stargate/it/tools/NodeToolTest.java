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
package io.stargate.it.tools;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeToolTest extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(NodeToolTest.class);

  private static File baseDir;
  private static File starterJar;
  private static StargateConnectionInfo stargate;

  @BeforeAll
  public static void init(StargateEnvironmentInfo stargateEnv) throws IOException {
    stargate = stargateEnv.nodes().get(0);
    starterJar = stargateEnv.starterJarFile();

    baseDir =
        new File(
                System.getProperty(
                    "stargate.basedir", System.getProperty("stargate.libdir") + "/.."))
            .getCanonicalFile();
  }

  protected CommandLine baseStarterCommand() throws IOException {
    CommandLine cmd = new CommandLine("java");
    cmd.addArgument("-Dstargate.libdir=" + starterJar.getParentFile().getCanonicalPath());

    // Java 11+ requires these flags to allow reflection to work
    cmd.addArgument("--add-exports");
    cmd.addArgument("java.base/jdk.internal.ref=ALL-UNNAMED");
    cmd.addArgument("--add-exports");
    cmd.addArgument("java.base/jdk.internal.misc=ALL-UNNAMED");

    cmd.addArgument("-jar");
    cmd.addArgument(starterJar.getCanonicalPath());

    cmd.addArgument("--cluster-version");
    cmd.addArgument(backend.clusterVersion());

    if (backend.isDse()) {
      cmd.addArgument("--dse");
    }

    return cmd;
  }

  protected Queue<String> nodetool(String... args) throws IOException {
    CommandLine cmd = baseStarterCommand();

    cmd.addArgument("--nodetool");
    cmd.addArgument("--host");
    cmd.addArgument(String.valueOf(stargate.seedAddress()));
    cmd.addArgument("--port");
    cmd.addArgument(String.valueOf(stargate.jmxPort()));

    for (String arg : args) {
      cmd.addArgument(arg);
    }

    Queue<String> output = new ConcurrentLinkedQueue<>();

    LogOutputStream out =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            LOG.info("nodetool> {}", line);
            output.add(line);
          }
        };

    LogOutputStream err =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            LOG.error("nodetool> {}", line);
          }
        };

    DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(baseDir);
    executor.setStreamHandler(new PumpStreamHandler(out, err));

    LOG.info("Starting NodeTool: {}", cmd);

    int rc = executor.execute(cmd);

    assertThat(rc).isEqualTo(0);

    return output;
  }

  @Test
  public void testDescribeCluster(StargateConnectionInfo stargate) throws IOException {
    Queue<String> output = nodetool("describecluster");
    assertThat(output).anyMatch(s -> s.contains("Schema versions"));
    assertThat(output).anyMatch(s -> s.contains(backend.clusterName()));
    assertThat(output).anyMatch(s -> s.contains(backend.seedAddress()));
    assertThat(output).anyMatch(s -> s.contains(stargate.seedAddress()));
  }
}
