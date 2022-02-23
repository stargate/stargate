/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.sgv2.common.cql.builder;

import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;

@Immutable
@Style(visibility = Style.ImplementationVisibility.PACKAGE)
public interface ValueModifier {
  Target target();

  Operation operation();

  Term<?> value();

  static ValueModifier set(String columnName, Object value) {
    return set(columnName, Term.of(value));
  }

  static ValueModifier set(String columnName, Term<?> value) {
    return of(Target.column(columnName), Operation.SET, value);
  }

  static ValueModifier marker(String columnName) {
    return of(Target.column(columnName), Operation.SET, Term.marker());
  }

  static ValueModifier of(Target target, Operation operation, Term<?> value) {
    return ImmutableValueModifier.builder()
        .target(target)
        .operation(operation)
        .value(value)
        .build();
  }

  enum Operation {
    SET,
    INCREMENT,
    APPEND,
    PREPEND,
    REMOVE,
  }

  @Immutable
  @Style(visibility = Style.ImplementationVisibility.PACKAGE)
  interface Target {
    String columnName();

    /** only set for UDT field access */
    @Nullable
    String fieldName();

    /** only set for map value access */
    @Nullable
    Term<?> mapKey();

    static Target column(String columnName) {
      return ImmutableTarget.builder().columnName(columnName).build();
    }

    static Target field(String columnName, String fieldName) {
      return ImmutableTarget.builder().columnName(columnName).fieldName(fieldName).build();
    }

    static Target mapValue(String columnName, Term<?> mapKey) {
      return ImmutableTarget.builder().columnName(columnName).mapKey(mapKey).build();
    }
  }
}
