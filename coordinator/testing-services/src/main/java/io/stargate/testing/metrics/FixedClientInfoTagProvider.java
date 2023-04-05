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

import io.micrometer.core.instrument.Tags;
import io.stargate.db.ClientInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;

/**
 * Simple {@link ClientInfoMetricsTagProvider} for testing that returns fixed tags for each client
 * info.
 */
public class FixedClientInfoTagProvider implements ClientInfoMetricsTagProvider {

  public static final String TAG_KEY = "clientInfo";
  public static final String TAG_VALUE = "fixed";

  @Override
  public Tags getClientInfoTags(ClientInfo clientInfo) {
    return Tags.of(TAG_KEY, TAG_VALUE);
  }
}
