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
package io.stargate.it.cql.protocolV4;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.it.cql.protocolV4.TypeSample.listOf;
import static io.stargate.it.cql.protocolV4.TypeSample.mapOfIntTo;
import static io.stargate.it.cql.protocolV4.TypeSample.mapToIntFrom;
import static io.stargate.it.cql.protocolV4.TypeSample.setOf;
import static io.stargate.it.cql.protocolV4.TypeSample.typeSample;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(customOptions = "applyProtocolVersion")
public class GeoTypeTest extends BaseIntegrationTest {

  private static List<TypeSample<?>> allTypes;

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @BeforeAll
  public static void validateAssumptions(ClusterConnectionInfo backend) {
    Assumptions.assumeTrue(backend.isDse(), "Test disabled when not running on DSE");
  }

  @BeforeAll
  public static void createSchema(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {

    allTypes = generateAllTypes(keyspaceId);

    // Creating a new table for each type is too slow, use a single table with all possible types:
    CreateTable createTableQuery = createTable("test").withPartitionKey("k", DataTypes.INT);
    for (TypeSample<?> sample : allTypes) {
      createTableQuery = createTableQuery.withColumn(sample.columnName, sample.cqlType);
    }

    session.execute(createTableQuery.asCql());
  }

  @SuppressWarnings("unused") // called by JUnit 5
  public static List<TypeSample<?>> getAllTypes() {
    return allTypes;
  }

  @DisplayName("Should write and read data type")
  @ParameterizedTest
  @MethodSource("getAllTypes")
  public <JavaTypeT> void should_write_and_read(TypeSample<JavaTypeT> sample, CqlSession session) {

    String insertQuery =
        insertInto("test").value("k", literal(1)).value(sample.columnName, bindMarker()).asCql();

    SimpleStatement simpleStatement = SimpleStatement.newInstance(insertQuery, sample.value);
    session.execute(simpleStatement);
    checkValue(sample, session);

    session.execute(BatchStatement.newInstance(BatchType.LOGGED).add(simpleStatement));
    checkValue(sample, session);

    session.execute(session.prepare(insertQuery).bind(sample.value));
    checkValue(sample, session);
  }

  private <JavaTypeT> void checkValue(TypeSample<JavaTypeT> sample, CqlSession session) {
    String selectQuery =
        selectFrom("test").column(sample.columnName).whereColumn("k").isEqualTo(literal(1)).asCql();
    Row row = session.execute(selectQuery).one();
    assertThat(row).isNotNull();
    assertThat(row.get(0, sample.javaType)).isEqualTo(sample.value);

    // Clean up for the following tests
    session.execute("DELETE FROM test WHERE k = 1");
  }

  private static List<TypeSample<?>> generateAllTypes(CqlIdentifier keyspaceId) {
    List<TypeSample<?>> primitiveTypes =
        Arrays.asList(
            typeSample(
                DseDataTypes.POINT, GenericType.of(Point.class), Point.fromCoordinates(1, 2)),
            typeSample(
                DseDataTypes.POLYGON,
                GenericType.of(Polygon.class),
                Polygon.fromPoints(
                    Point.fromCoordinates(0, 0),
                    Point.fromCoordinates(0, 1),
                    Point.fromCoordinates(1, 1))),
            typeSample(
                DseDataTypes.LINE_STRING,
                GenericType.of(LineString.class),
                LineString.fromPoints(Point.fromCoordinates(0, 0), Point.fromCoordinates(0, 1))));

    List<TypeSample<?>> allTypes = new ArrayList<>();
    for (TypeSample<?> type : primitiveTypes) {
      // Generate additional samples from each primitive
      allTypes.add(type);
      allTypes.add(listOf(type));
      allTypes.add(setOf(type));
      allTypes.add(mapToIntFrom(type));
      allTypes.add(mapOfIntTo(type));
      // TODO: there are some odd codec lookup errors in tupleOfIntAnd(...) and udtOfIntAnd(...)
      // TODO: allTypes.add(tupleOfIntAnd(type));
      // TODO: allTypes.add(udtOfIntAnd(type, keyspaceId));
    }
    return allTypes;
  }
}
