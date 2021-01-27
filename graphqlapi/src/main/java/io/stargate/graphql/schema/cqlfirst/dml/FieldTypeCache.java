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
package io.stargate.graphql.schema.cqlfirst.dml;

import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableListType;
import io.stargate.graphql.schema.cqlfirst.dml.types.scalars.CustomScalars;
import java.util.HashMap;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * Caches a category of GraphQL field types, corresponding to table columns or UDT fields.
 *
 * <p>There are different categories, each implemented by a subclass. Note that this cache does not
 * contain table types, they are handled as top-level entities in {@link DmlSchemaBuilder}.
 *
 * @param <GraphqlT> the returned GraphQL type.
 */
@NotThreadSafe
abstract class FieldTypeCache<GraphqlT> {

  protected final NameMapping nameMapping;
  private final Map<ColumnType, GraphqlT> types = new HashMap<>();

  FieldTypeCache(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  GraphqlT get(ColumnType type) {
    type = normalize(type);
    return computeIfAbsent(type);
  }

  // Reimplement HashMap#computeIfAbsent because it has a bug in JDK 8 (if the compute method adds
  // other entries, they won't be visible).
  // See https://bugs.openjdk.java.net/browse/JDK-8071667
  private GraphqlT computeIfAbsent(ColumnType type) {
    GraphqlT result = types.get(type);
    if (result == null) {
      result = compute(type);
      types.put(type, result);
    }
    return result;
  }

  /**
   * Different column types can be mapped to the same GraphQL type. Instead of having to look up if
   * the GraphQL type already exists, we treat those CQL types as equal for the purpose of
   * cql->graphql type mapping.
   */
  private ColumnType normalize(ColumnType type) {
    // Frozen-ness does not matter. We want frozen and non-frozen versions of a CQL type to be
    // mapped to the same GraphQL type.
    type = type.frozen(false);

    // CQL set and list are both converted to GraphQL list.
    if (type.isSet()) {
      type = ImmutableListType.builder().addAllParameters(type.parameters()).build();
    }

    // CQL text and varchar use the same GraphQL type.
    if (type == Type.Varchar) {
      type = Type.Text;
    }

    return type;
  }

  /**
   * Computes a result on a cache miss. If you need nested types, use {@link #get} to obtain them,
   * in case they were already cached.
   */
  protected abstract GraphqlT compute(ColumnType columnType);

  protected GraphQLScalarType getScalar(Type type) {
    switch (type) {
      case Ascii:
        return CustomScalars.ASCII;
      case Bigint:
        return CustomScalars.BIGINT;
      case Blob:
        return CustomScalars.BLOB;
      case Boolean:
        return Scalars.GraphQLBoolean;
      case Counter:
        return CustomScalars.COUNTER;
      case Decimal:
        return CustomScalars.DECIMAL;
      case Double:
        // GraphQL's Float is a signed double‐precision fractional value
        return Scalars.GraphQLFloat;
      case Duration:
        return CustomScalars.DURATION;
      case Float:
        // Use a custom scalar named "Float32"
        return CustomScalars.FLOAT;
      case Int:
        return Scalars.GraphQLInt;
      case Smallint:
        return CustomScalars.SMALLINT;
      case Tinyint:
        return CustomScalars.TINYINT;
      case Text:
      case Varchar:
        return Scalars.GraphQLString;
      case Timestamp:
        return CustomScalars.TIMESTAMP;
      case Uuid:
        return CustomScalars.UUID;
      case Varint:
        return CustomScalars.VARINT;
      case Timeuuid:
        return CustomScalars.TIMEUUID;
      case Inet:
        return CustomScalars.INET;
      case Date:
        return CustomScalars.DATE;
      case Time:
        return CustomScalars.TIME;
      default:
        throw new IllegalArgumentException("Unsupported primitive type " + type);
    }
  }
}
