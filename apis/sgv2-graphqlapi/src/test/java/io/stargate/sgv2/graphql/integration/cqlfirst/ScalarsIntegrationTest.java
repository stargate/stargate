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
package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import io.stargate.sgv2.graphql.schema.Uuids;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ScalarsIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"Scalars\" (\n"
            + "    id uuid PRIMARY KEY,\n"
            + "    asciivalue ascii,\n"
            + "    bigintvalue bigint,\n"
            + "    blobvalue blob,\n"
            + "    booleanvalue boolean,\n"
            + "    datevalue date,\n"
            + "    decimalvalue decimal,\n"
            + "    doublevalue double,\n"
            + "    durationvalue duration,\n"
            + "    floatvalue float,\n"
            + "    inetvalue inet,\n"
            + "    intvalue int,\n"
            + "    smallintvalue smallint,\n"
            + "    textvalue text,\n"
            + "    timevalue time,\n"
            + "    timestampvalue timestamp,\n"
            + "    timeuuidvalue timeuuid,\n"
            + "    tinyintvalue tinyint,\n"
            + "    uuidvalue uuid,\n"
            + "    varcharvalue varchar,\n"
            + "    varintvalue varint\n"
            + ")");
  }

  @ParameterizedTest
  @MethodSource("getScalarValues")
  public void shouldSupportScalar(String fieldName, Object value) {

    UUID id = UUID.randomUUID();
    String graphqlValue =
        (value instanceof String) ? String.format("\"%s\"", value) : value.toString();

    // When writing a value of this type:
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
            String.format(
                "mutation { updateScalars(value: {id: \"%s\", %s: %s}) { applied } }",
                id, fieldName, graphqlValue));
    assertThat(JsonPath.<Boolean>read(response, "$.updateScalars.applied")).isTrue();

    // Should read back the same value:
    response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
            String.format("{ Scalars(value: {id: \"%s\"}) { values { %s } } }", id, fieldName));
    String fieldPath = String.format("$.Scalars.values[0].%s", fieldName);
    assertThat(JsonPath.<Object>read(response, fieldPath)).isEqualTo(value);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getScalarValues() {
    return Stream.of(
        arguments("asciivalue", "abc"),
        arguments("bigintvalue", "-9223372036854775807"),
        arguments("blobvalue", "AQID//7gEiMB"),
        arguments("booleanvalue", true),
        arguments("booleanvalue", false),
        arguments("datevalue", "2005-08-05"),
        arguments("decimalvalue", "-0.123456"),
        arguments("doublevalue", -1D),
        arguments("durationvalue", "12h30m"),
        // Serialized as JSON numbers
        arguments("floatvalue", 1.1234D),
        arguments("inetvalue", "8.8.8.8"),
        arguments("intvalue", 1),
        // Serialized as JSON Number
        arguments("smallintvalue", 32_767),
        arguments("textvalue", "abc123", "'abc123'"),
        arguments("textvalue", ""),
        arguments("timevalue", "23:59:31.123456789"),
        arguments("timestampvalue", formatInstant(Instant.now())),
        arguments("tinyintvalue", -128),
        arguments("tinyintvalue", 1),
        arguments("timeuuidvalue", Uuids.timeBased().toString()),
        arguments("uuidvalue", "f3abdfbf-479f-407b-9fde-128145bd7bef"),
        arguments("varintvalue", "92233720368547758070000"));
  }

  private static String formatInstant(Instant instant) {
    return TIMESTAMP_FORMAT.get().format(Date.from(instant));
  }

  private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(
          () -> {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            parser.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
            return parser;
          });
}
