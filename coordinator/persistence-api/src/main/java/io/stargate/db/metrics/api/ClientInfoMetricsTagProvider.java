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

package io.stargate.db.metrics.api;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.db.ClientInfo;
import io.stargate.db.DriverInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides extra micrometer {@link Tags} based on the {@link ClientInfo}.
 *
 * <p>Note that each method implementation must always return constant amount of tags regardless of
 * the input.
 */
public interface ClientInfoMetricsTagProvider {

  /**
   * Returns default interface implementation, which returns empty tags or with driver info for all
   * methods.
   */
  ClientInfoMetricsTagProvider DEFAULT = new ClientInfoMetricsTagProvider() {};

  /**
   * Returns tags for a {@link ClientInfo}.
   *
   * <p>Note that the implementation must return constant amount of tags for any input. IF the
   * client info is populated with driver info, the tags should be populated with the driver name
   * and version. Otherwise, the tags should be populated with "unknown" values.
   *
   * @param clientInfo {@link ClientInfo}
   * @return Tags
   */
  default Tags getClientInfoTagsByDriver(ClientInfo clientInfo) {
    List<Tag> tags = new ArrayList<>(2);
    if (clientInfo.driverInfo().isPresent()) {
      DriverInfo driverInfo = clientInfo.driverInfo().get();
      tags.add(Tag.of("driverName", driverInfo.name()));
      tags.add(Tag.of("driverVersion", driverInfo.version().orElse("unknown")));
    } else {
      tags.add(Tag.of("driverName", "unknown"));
      tags.add(Tag.of("driverVersion", "unknown"));
    }
    return Tags.of(tags);
  }

  /**
   * Returns tags for a {@link ClientInfo}.
   *
   * <p>Note that the implementation must return constant amount of tags for any input.
   *
   * @param clientInfo {@link ClientInfo}
   * @return Tags
   */
  default Tags getClientInfoTags(ClientInfo clientInfo) {
    return Tags.empty();
  }
}
