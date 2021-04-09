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
package io.stargate.it.storage;

import io.stargate.it.exec.OutputListener;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

public class LogCollector implements OutputListener, Store.CloseableResource {

  private final Map<Integer, Queue<String>> outputs = new ConcurrentHashMap<>();
  private final StargateEnvironmentInfo stargate;

  public LogCollector(StargateEnvironmentInfo stargate) {
    this.stargate = stargate;
    stargate.addStdOutListener(this);
  }

  @Override
  public void close() throws Exception {
    stargate.removeStdOutListener(this);
  }

  @Override
  public void processLine(int node, String output) {
    Queue<String> queue = queue(node);
    queue.add(output);
  }

  private Queue<String> queue(int node) {
    return outputs.computeIfAbsent(node, __ -> new ConcurrentLinkedQueue<>());
  }

  public void reset() {
    outputs.clear();
  }

  public List<String> filter(int node, Pattern pattern, int group) {
    return queue(node).stream()
        .map(
            s -> {
              Matcher matcher = pattern.matcher(s);
              if (matcher.matches()) {
                return matcher.group(group);
              } else {
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public List<String> filter(int node, Pattern pattern, int group, int numExpectedMessages) {
    return Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> filter(node, pattern, group), list -> list.size() >= numExpectedMessages);
  }
}
