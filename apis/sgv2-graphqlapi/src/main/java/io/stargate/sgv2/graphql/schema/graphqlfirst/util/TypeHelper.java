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
package io.stargate.sgv2.graphql.schema.graphqlfirst.util;

import static graphql.language.ListType.newListType;
import static graphql.language.NonNullType.newNonNullType;
import static graphql.language.TypeName.newTypeName;

import com.google.common.collect.ImmutableSet;
import graphql.Scalars;
import graphql.language.ListType;
import graphql.language.Node;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.Map;
import java.util.Set;

public class TypeHelper {

  private static final Set<String> UUID_TYPE_NAMES =
      ImmutableSet.of(
          Scalars.GraphQLID.getName(),
          CqlScalar.UUID.getGraphqlType().getName(),
          CqlScalar.TIMEUUID.getGraphqlType().getName());

  /**
   * Tests if the two types represent the exact same GraphQL type: unlike {@link
   * Type#isEqualTo(Node)}, this method does compare the children.
   */
  public static boolean deepEquals(Type<?> type1, Type<?> type2) {
    if (type1 instanceof ListType && type2 instanceof ListType) {
      return deepEquals(((ListType) type1).getType(), ((ListType) type2).getType());
    }
    if (type1 instanceof NonNullType && type2 instanceof NonNullType) {
      return deepEquals(((NonNullType) type1).getType(), ((NonNullType) type2).getType());
    }
    if (type1 instanceof TypeName && type2 instanceof TypeName) {
      return type1.isEqualTo(type2);
    }
    return false;
  }

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

  /** Recursively replaces every type that maps to an entity by its input type. */
  public static Type<?> toInput(Type<?> type, Map<String, EntityModel> entities) {
    if (type instanceof NonNullType) {
      Type<?> elementType = toInput(((NonNullType) type).getType(), entities);
      return newNonNullType(elementType).build();
    }
    if (type instanceof ListType) {
      Type<?> elementType = toInput(((ListType) type).getType(), entities);
      return newListType(elementType).build();
    }
    assert type instanceof TypeName;
    String typeName = ((TypeName) type).getName();
    EntityModel entity = entities.get(typeName);
    if (entity == null) {
      return type;
    }
    String inputTypeName =
        entity
            .getInputTypeName()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("type %s has no input type", typeName)));
    return newTypeName(inputTypeName).build();
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
