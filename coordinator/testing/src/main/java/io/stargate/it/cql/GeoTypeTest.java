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
package io.stargate.it.cql;

import static io.stargate.it.cql.TypeSample.listOf;
import static io.stargate.it.cql.TypeSample.mapOfIntTo;
import static io.stargate.it.cql.TypeSample.mapToIntFrom;
import static io.stargate.it.cql.TypeSample.setOf;
import static io.stargate.it.cql.TypeSample.typeSample;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.stargate.it.driver.WithProtocolVersion;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

public abstract class GeoTypeTest extends AbstractTypeTest {
  @BeforeAll
  public static void validateAssumptions(ClusterConnectionInfo backend) {
    Assumptions.assumeTrue(backend.isDse(), "Test disabled when not running on DSE");
  }

  @Override
  protected List<TypeSample<?>> generateAllTypes(CqlIdentifier keyspaceId) {
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

  @WithProtocolVersion("V4")
  public static class WithV4ProtocolVersionTest extends GeoTypeTest {}

  @WithProtocolVersion("V5")
  public static class WithV5ProtocolVersionTest extends GeoTypeTest {}
}
