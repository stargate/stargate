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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.jayway.jsonpath.JsonPath;
import io.stargate.db.schema.Column;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
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
          + ")"
    })
public class ScalarsTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;
  private static CqlIdentifier KEYSPACE_ID;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    KEYSPACE_ID = keyspaceId;
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @ParameterizedTest
  @MethodSource("getScalarValues")
  public void shouldSupportScalar(Column.Type type, Object value) {

    UUID id = UUID.randomUUID();
    String fieldName = type.name().toLowerCase() + "value";
    String graphqlValue =
        (value instanceof String) ? String.format("\"%s\"", value) : value.toString();

    // When writing a value of this type:
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            KEYSPACE_ID,
            String.format(
                "mutation { updateScalars(value: {id: \"%s\", %s: %s}) { applied } }",
                id, fieldName, graphqlValue));
    assertThat(JsonPath.<Boolean>read(response, "$.updateScalars.applied")).isTrue();

    // Should read back the same value:
    response =
        CLIENT.executeDmlQuery(
            KEYSPACE_ID,
            String.format("{ Scalars(value: {id: \"%s\"}) { values { %s } } }", id, fieldName));
    String fieldPath = String.format("$.Scalars.values[0].%s", fieldName);
    assertThat(JsonPath.<Object>read(response, fieldPath)).isEqualTo(value);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getScalarValues() {
    return Stream.of(
        arguments(Column.Type.Ascii, "abc"),
        arguments(Column.Type.Bigint, "-9223372036854775807"),
        arguments(Column.Type.Blob, "AQID//7gEiMB"),
        arguments(Column.Type.Boolean, true),
        arguments(Column.Type.Boolean, false),
        arguments(Column.Type.Date, "2005-08-05"),
        arguments(Column.Type.Decimal, "-0.123456"),
        arguments(Column.Type.Double, -1D),
        arguments(Column.Type.Duration, "12h30m"),
        // Serialized as JSON numbers
        arguments(Column.Type.Float, 1.1234D),
        arguments(Column.Type.Inet, "8.8.8.8"),
        arguments(Column.Type.Int, 1),
        // Serialized as JSON Number
        arguments(Column.Type.Smallint, 32_767),
        arguments(Column.Type.Text, "abc123", "'abc123'"),
        arguments(Column.Type.Text, ""),
        arguments(Column.Type.Time, "23:59:31.123456789"),
        arguments(Column.Type.Timestamp, formatInstant(now())),
        arguments(Column.Type.Tinyint, -128),
        arguments(Column.Type.Tinyint, 1),
        arguments(Column.Type.Timeuuid, Uuids.timeBased().toString()),
        arguments(Column.Type.Uuid, "f3abdfbf-479f-407b-9fde-128145bd7bef"),
        arguments(Column.Type.Varint, "92233720368547758070000"));
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
