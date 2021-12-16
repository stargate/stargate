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
package io.stargate.db.query.builder;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkArgument;
import static io.stargate.db.query.BindMarker.markerFor;
import static java.lang.String.format;

import io.stargate.db.query.BindMarker;
import io.stargate.db.query.Predicate;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ColumnUtils;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.immutables.value.Value.Style.ImplementationVisibility;

@org.immutables.value.Value.Immutable(prehash = true)
@org.immutables.value.Value.Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class BuiltCondition {

  public abstract LHS lhs();

  public abstract Predicate predicate();

  public abstract Value<?> value();

  public static BuiltCondition of(String columnName, Predicate predicate, Object value) {
    return of(LHS.column(columnName), predicate, value);
  }

  public static BuiltCondition of(LHS lhs, Predicate predicate, Object value) {
    return ImmutableBuiltCondition.builder()
        .lhs(lhs)
        .predicate(predicate)
        .value(Value.of(value))
        .build();
  }

  public static BuiltCondition ofMarker(String columnName, Predicate predicate) {
    return ofMarker(LHS.column(columnName), predicate);
  }

  public static BuiltCondition ofMarker(LHS lhs, Predicate predicate) {
    return ImmutableBuiltCondition.builder()
        .lhs(lhs)
        .predicate(predicate)
        .value(Value.marker())
        .build();
  }

  public static BuiltCondition ofModifier(ValueModifier modifier) {
    return ImmutableBuiltCondition.builder()
        .lhs(LHS.column(modifier.target().columnName()))
        .predicate(Predicate.EQ)
        .value(modifier.value())
        .build();
  }

  void addToBuilder(
      AbstractTable table, QueryStringBuilder builder, Consumer<BindMarker> onMarker) {
    Column receiver = lhs().appendToBuilder(table, builder, onMarker);
    builder.append(predicate().toString());
    ColumnType type = receiver.type();
    assert type != null;
    BindMarker marker;
    switch (predicate()) {
      case EQ:
        if (lhs().isMapAccess()) {
          marker = markerFor(format("value(%s)", receiver.name()), type);
          builder.append(marker, value());
          break;
        }
        // fallthrough on purpose for "normal" EQ
      case LT:
      case GT:
      case LTE:
      case GTE:
      case NEQ:
        marker = markerFor(receiver);
        builder.append(marker, value());
        break;
      case IN:
        marker = markerFor(format("IN(%s)", receiver.name()), Type.List.of(type));
        builder.appendInValue(marker, value());
        break;
      case CONTAINS:
        checkArgument(
            type.isCollection(),
            "CONTAINS predicate on %s is invalid: CONTAINS can only apply to a collection, "
                + "but %s is of type %s",
            receiver.cqlName(),
            receiver.cqlName(),
            type.cqlDefinition());
        ColumnType valueType = type.isMap() ? type.parameters().get(1) : type.parameters().get(0);
        marker = markerFor(format("value(%s)", receiver.name()), valueType);
        builder.append(marker, value());
        break;
      case CONTAINS_KEY:
        checkArgument(
            type.isMap(),
            "CONTAINS KEY predicate on %s is invalid: CONTAINS KEY can only apply to a map, "
                + "but %s is of type %s",
            receiver.cqlName(),
            receiver.cqlName(),
            type.cqlDefinition());
        marker = markerFor(format("key(%s)", receiver.name()), type.parameters().get(0));
        builder.append(marker, value());
        break;
      default:
        throw new UnsupportedOperationException();
    }
    onMarker.accept(marker);
  }

  @Override
  public String toString() {
    return format("%s %s %s", lhs(), predicate(), value());
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
  public abstract static class LHS {
    public static LHS column(String columnName) {
      return new ColumnName(columnName);
    }

    public static LHS mapAccess(String columnName, Object key) {
      return new MapElement(columnName, Value.of(key));
    }

    public static LHS mapAccess(String columnName) {
      return new MapElement(columnName, Value.marker());
    }

    public static LHS columnTuple(String... columnNames) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    public static LHS token(String... columnNames) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    abstract Column appendToBuilder(
        AbstractTable table, QueryStringBuilder builder, Consumer<BindMarker> onMarker);

    abstract String columnName();

    Optional<Value<?>> value() {
      return Optional.empty();
    }

    boolean isColumnName() {
      return false;
    }

    boolean isMapAccess() {
      return false;
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
      boolean isColumnName() {
        return true;
      }

      @Override
      Column appendToBuilder(
          AbstractTable table, QueryStringBuilder builder, Consumer<BindMarker> onMarker) {
        Column column = table.existingColumn(columnName);
        builder.append(column);
        return column;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof ColumnName)) {
          return false;
        }
        ColumnName that = (ColumnName) o;
        return columnName.equals(that.columnName);
      }

      @Override
      public int hashCode() {
        return Objects.hash(columnName);
      }

      @Override
      public String toString() {
        return ColumnUtils.maybeQuote(columnName);
      }
    }

    static final class MapElement extends LHS {
      private final String columnName;
      private final Value<?> keyValue;

      private MapElement(String columnName, Value<?> keyValue) {
        this.columnName = columnName;
        this.keyValue = keyValue;
      }

      @Override
      String columnName() {
        return columnName;
      }

      Value<?> keyValue() {
        return keyValue;
      }

      @Override
      Optional<Value<?>> value() {
        return Optional.of(keyValue);
      }

      @Override
      Column appendToBuilder(
          AbstractTable table, QueryStringBuilder builder, Consumer<BindMarker> onMarker) {
        Column column = table.existingColumn(columnName);
        ColumnType type = column.type();
        assert type != null;
        checkArgument(
            type.isMap(),
            "Invalid access by key on column %s of type %s: accessing keys only works on map",
            column.cqlName(),
            type.cqlDefinition());
        builder.append(column).appendForceNoSpace("[");
        ColumnType keyType = type.parameters().get(0);
        BindMarker keyMarker = markerFor(format("key(%s)", column.name()), keyType);
        builder.append(keyMarker, keyValue).append("]");
        onMarker.accept(keyMarker);
        return Column.create(format("value(%s)", column.name()), type.parameters().get(1));
      }

      @Override
      boolean isMapAccess() {
        return true;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof MapElement)) {
          return false;
        }
        MapElement that = (MapElement) o;
        return columnName.equals(that.columnName) && keyValue.equals(that.keyValue);
      }

      @Override
      public int hashCode() {
        return Objects.hash(columnName, keyValue);
      }

      @Override
      public String toString() {
        return format("%s[%s]", ColumnUtils.maybeQuote(columnName), keyValue);
      }
    }
  }
}
