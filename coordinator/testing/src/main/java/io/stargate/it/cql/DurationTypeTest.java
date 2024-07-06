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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static io.stargate.it.cql.TypeSample.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.stargate.it.driver.WithProtocolVersion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DurationTypeTest extends AbstractTypeTest {
  @Override
  protected List<TypeSample<?>> generateAllTypes(CqlIdentifier keyspaceId) {
    List<TypeSample<?>> primitiveTypes =
        Arrays.asList(
            typeSample(
                DataTypes.DURATION,
                GenericType.of(CqlDuration.class),
                CqlDuration.newInstance(3, 2, 1)));

    List<TypeSample<?>> allTypes = new ArrayList<>();
    for (TypeSample<?> type : primitiveTypes) {
      // Generate additional samples from each primitive
      allTypes.add(type);
      allTypes.add(listOf(type));
      // allTypes.add(setOf(type));        // Duration type cannot be used in sets
      // allTypes.add(mapToIntFrom(type)); // Duration type cannot be used as keys in maps
      allTypes.add(mapOfIntTo(type));
      allTypes.add(tupleOfIntAnd(type));
      //      allTypes.add(tupleOfIntAnd(type));
      //      allTypes.add(udtOfIntAnd(type, keyspaceId));
    }
    return allTypes;
  }

  @WithProtocolVersion("V5")
  public static class WithV5ProtocolVersionTest extends DurationTypeTest {}
}
