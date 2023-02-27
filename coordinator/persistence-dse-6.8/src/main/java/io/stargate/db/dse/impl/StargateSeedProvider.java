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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.db.dse.impl;

import io.stargate.db.datastore.common.AbstractStargateSeedProvider;
import java.util.Map;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SeedProvider;

public class StargateSeedProvider extends AbstractStargateSeedProvider implements SeedProvider {

  public StargateSeedProvider(Map<String, String> args) {
    super(args);
  }

  @Override
  protected RuntimeException getNoSeedsException() {
    return new ConfigurationException("seeds arg required");
  }
}
