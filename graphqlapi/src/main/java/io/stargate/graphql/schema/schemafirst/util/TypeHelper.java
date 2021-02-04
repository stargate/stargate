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

import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;

public class TypeHelper {

  public static boolean isGraphqlId(Type<?> type) {
    type = unwrapNonNull(type);
    return type instanceof TypeName && ((TypeName) type).getName().equals("ID");
  }

  /** If the type is {@code !T}, return {@code T}, otherwise the type unchanged. */
  public static Type<?> unwrapNonNull(Type<?> type) {
    return type instanceof NonNullType ? ((NonNullType) type).getType() : type;
  }

  private TypeHelper() {
    // hide constructor for utility class
  }
}
