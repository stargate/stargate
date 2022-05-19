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

package io.stargate.sgv2.docsapi.api.v2.example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.docsapi.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(StargateTestResource.class)
@TestProfile(IntegrationTestProfile.class)
class ExampleResourceIntegrationTest {

  @Test
  public void resourceLoads() {}
}
