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

import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.ColumnUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Immutable
@Style(visibility = ImplementationVisibility.PACKAGE)
public interface BuiltCondition {

  LHS lhs();

  Predicate predicate();

  Term value();

  static BuiltCondition of(String columnName, Predicate predicate, Object value) {
    return of(columnName, predicate, Term.of(value));
  }

  static BuiltCondition of(String columnName, Predicate predicate, Term value) {
    return of(LHS.column(columnName), predicate, value);
  }

  static BuiltCondition of(LHS lhs, Predicate predicate, Object value) {
    return of(lhs, predicate, Term.of(value));
  }

  static BuiltCondition of(LHS lhs, Predicate predicate, Term value) {
    return ImmutableBuiltCondition.builder().lhs(lhs).predicate(predicate).value(value).build();
  }

  /**
   * Represents the left hand side of a condition.
   *
   * <p>This is usually a column name, but technically can be:
   *
   * <ul>
   *   <li>a column name ("c = ...")
   *   <li>a specific element in a map column ("c[v] = ...")
   *   <li>a tuple of column name ("(c, d, e) = ...")
   *   <li>the token of a tuple of column name ("TOKEN(c, d, e) = ...")
   * </ul>
   */
  abstract class LHS {
    public static LHS column(String columnName) {
      return new ColumnName(columnName);
    }

    public static LHS mapAccess(String columnName, Object key) {
      return new MapElement(columnName, Term.of(key));
    }

    public static LHS columnTuple(String... columnNames) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    public static LHS token(String... columnNames) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    abstract void appendToBuilder(
        StringBuilder builder, Map<Marker, Value> markers, List<Value> boundValues);

    abstract String columnName();

    Optional<Term> value() {
      return Optional.empty();
    }

    static final class ColumnName extends LHS {
      private final String columnName;

      private ColumnName(String columnName) {
        this.columnName = columnName;
      }

      @Override
      String columnName() {
        return columnName;
      }

      @Override
      void appendToBuilder(
          StringBuilder builder, Map<Marker, Value> markers, List<Value> boundValues) {
        builder.append(ColumnUtils.maybeQuote(columnName));
      }
    }

    static final class MapElement extends LHS {
      private final String columnName;
      private final Term keyValue;

      MapElement(String columnName, Term keyValue) {
        this.columnName = columnName;
        this.keyValue = keyValue;
      }

      @Override
      String columnName() {
        return columnName;
      }

      Term keyValue() {
        return keyValue;
      }

      @Override
      Optional<Term> value() {
        return Optional.of(keyValue);
      }

      @Override
      void appendToBuilder(
          StringBuilder builder, Map<Marker, Value> markers, List<Value> boundValues) {
        builder
            .append(ColumnUtils.maybeQuote(columnName))
            .append('[')
            .append(QueryBuilderImpl.formatValue(keyValue, markers, boundValues))
            .append(']');
      }
    }
  }
}
