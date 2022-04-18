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

package io.stargate.sgv2.docsapi.api.common.metrics.configuration;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MicrometerConfigurationTest.Profile.class)
class MicrometerConfigurationTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.metrics.global-tags.module", "test-module")
          .put("stargate.metrics.global-tags.extra-tag", "extra-value")
          .build();
    }
  }

  @Test
  public void globalTags() {
    // collect metrics
    String result = given().when().get("/metrics").then().statusCode(200).extract().asString();

    List<String> meteredLines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .collect(Collectors.toList());

    // confirm existence in one, not all support global tags
    assertThat(meteredLines)
        .anySatisfy(
            metric ->
                assertThat(metric)
                    .contains("module=\"test-module\"")
                    .contains("extra_tag=\"extra-value\""));
  }
}
