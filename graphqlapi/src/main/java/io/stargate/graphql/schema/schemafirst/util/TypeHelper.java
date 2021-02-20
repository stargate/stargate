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
package io.stargate.graphql.schema.schemafirst.util;

import com.google.common.collect.ImmutableSet;
import graphql.Scalars;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.scalars.CqlScalar;
import java.util.Set;

public class TypeHelper {

  private static final Set<String> UUID_TYPE_NAMES =
      ImmutableSet.of(
          Scalars.GraphQLID.getName(),
          CqlScalar.UUID.getGraphqlType().getName(),
          CqlScalar.TIMEUUID.getGraphqlType().getName());

  public static boolean mapsToUuid(Type<?> type) {
    type = unwrapNonNull(type);
    if (type instanceof TypeName) {
      TypeName typeName = (TypeName) type;
      return UUID_TYPE_NAMES.contains(typeName.getName());
    }
    return false;
  }

  public static boolean isGraphqlId(Type<?> type) {
    type = unwrapNonNull(type);
    return type instanceof TypeName
        && Scalars.GraphQLID.getName().equals(((TypeName) type).getName());
  }

  /** If the type is {@code !T}, return {@code T}, otherwise the type unchanged. */
  public static Type<?> unwrapNonNull(Type<?> type) {
    return type instanceof NonNullType ? ((NonNullType) type).getType() : type;
  }

  public static String format(Type<?> type) {
    if (type instanceof NonNullType) {
      return format(((NonNullType) type).getType()) + '!';
    }
    if (type instanceof ListType) {
      return "[" + format(((ListType) type).getType()) + "]";
    }
    assert type instanceof TypeName;
    return ((TypeName) type).getName();
  }

  private TypeHelper() {
    // hide constructor for utility class
  }
}
