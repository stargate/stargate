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
package io.stargate.db.datastore.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractStargateSeedProvider {

  private final List<InetAddress> seeds;

  public AbstractStargateSeedProvider(Map<String, String> args) {
    if (!args.containsKey("seeds")) {
      throw getNoSeedsException();
    }

    seeds =
        Arrays.stream(args.get("seeds").split(","))
            .map(
                s -> {
                  try {
                    return InetAddress.getAllByName(s.trim());
                  } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                  }
                })
            .flatMap(Arrays::stream)
            .collect(Collectors.toList());
  }

  protected abstract RuntimeException getNoSeedsException();

  public List<InetAddress> getSeeds() {
    return seeds;
  }
}
