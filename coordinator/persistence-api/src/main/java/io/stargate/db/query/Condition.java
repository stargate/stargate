package io.stargate.db.query;

import static java.lang.String.format;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;

@Immutable
public interface Condition {
  LHS lhs();

  Predicate predicate();

  @Nullable
  TypedValue value();

  /**
   * Represents the left hand side of a condition.
   *
   * <p>This is usually a column, but technically can be:
   *
   * <ul>
   *   <li>a column ("c = ...")
   *   <li>a specific element in a map column ("c[v] = ...")
   *   <li>a tuple of column name ("(c, d, e) = ...")
   *   <li>the token of a tuple of column name ("TOKEN(c, d, e) = ...")
   * </ul>
   */
  abstract class LHS {
    enum Kind {
      COLUMN,
      MAP_ACCESS,
      TUPLE,
      TOKEN
    }

    private final Kind kind;

    private LHS(Kind kind) {
      this.kind = kind;
    }

    public Kind kind() {
      return kind;
    }

    public abstract ColumnType valueType();

    public boolean isUnset() {
      return false;
    }

    public static LHS column(Column column) {
      return new SimpleColumn(column);
    }

    public static LHS mapAccess(Column column, TypedValue key) {
      return new MapAccess(column, key);
    }

    public static LHS columnTuple(List<Column> columns) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    public static LHS token(List<Column> columns) {
      // Not yet needed, but we should add it someday
      throw new UnsupportedOperationException();
    }

    public static class SimpleColumn extends LHS {
      private final Column column;

      private SimpleColumn(Column column) {
        super(Kind.COLUMN);
        this.column = column;
      }

      public Column column() {
        return column;
      }

      @Override
      public String toString() {
        return column.cqlName();
      }

      @Override
      public ColumnType valueType() {
        return column.type();
      }
    }

    private static class MapAccess extends LHS {
      private final Column column;
      private final TypedValue keyValue;

      private MapAccess(Column column, TypedValue keyValue) {
        super(Kind.MAP_ACCESS);
        Preconditions.checkArgument(column.type().isMap());
        Preconditions.checkArgument(column.type().parameters().get(0).equals(keyValue.type()));
        this.column = column;
        this.keyValue = keyValue;
      }

      public Column column() {
        return column;
      }

      @Override
      public ColumnType valueType() {
        return column.type().parameters().get(1);
      }

      @Override
      public boolean isUnset() {
        return keyValue.isUnset();
      }

      @Override
      public String toString() {
        return format("%s[%s]", column.cqlName(), keyValue);
      }
    }
  }
}
