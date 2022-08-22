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
package io.stargate.sgv2.docsapi.testprofiles;

import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Map;

public class MaxDepth8TestProfile implements NoGlobalResourcesTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of("stargate.document.max-depth", "8");
  }
}
