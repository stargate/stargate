/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.testing.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.db.ClientInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple {@link ClientInfoMetricsTagProvider} for testing that returns fixed tags for each client
 * info.
 */
public class FixedClientInfoTagProvider implements ClientInfoMetricsTagProvider {

  public static final String TAG_KEY = "clientInfo";
  public static final String TAG_VALUE = "fixed";
  public static final String TAG_KEY_DRIVER_NAME = "driverName";
  public static final String TAG_VALUE_DRIVER_NAME = "java";
  public static final String TAG_KEY_DRIVER_VERSION = "driverVersion";
  public static final String TAG_VALUE_DRIVER_VERSION = "1.0.0";

  @Override
  public Tags getClientInfoTags(ClientInfo clientInfo) {
    return Tags.of(TAG_KEY, TAG_VALUE);
  }

  @Override
  public Tags getClientInfoTagsByDriver(ClientInfo clientInfo) {
    List<Tag> tags = new ArrayList<>(2);
    tags.add(Tag.of(TAG_KEY_DRIVER_NAME, TAG_VALUE_DRIVER_NAME));
    tags.add(Tag.of(TAG_KEY_DRIVER_VERSION, TAG_VALUE_DRIVER_VERSION));
    return Tags.of(tags);
  }
}
