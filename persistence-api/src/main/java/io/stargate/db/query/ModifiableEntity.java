package io.stargate.db.query;

import static java.lang.String.format;

import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.UserDefinedType;
import java.util.Objects;

public abstract class ModifiableEntity {
  public enum Kind {
    COLUMN,
    UDT_FIELD,
    MAP_VALUE
  }

  private final Kind kind;
  protected final io.stargate.db.schema.Column column;

  private ModifiableEntity(Kind kind, io.stargate.db.schema.Column column) {
    this.kind = kind;
    this.column = column;
  }

  public Kind kind() {
    return kind;
  }

  public abstract ColumnType type();

  public static Column of(io.stargate.db.schema.Column column) {
    return new Column(column);
  }

  public static UdtField udtType(io.stargate.db.schema.Column udtColumn, String fieldName) {
    return new UdtField(udtColumn, fieldName);
  }

  public static MapValue mapValue(io.stargate.db.schema.Column mapColumn, TypedValue key) {
    return new MapValue(mapColumn, key);
  }

  public static class Column extends ModifiableEntity {
    private Column(io.stargate.db.schema.Column column) {
      super(Kind.COLUMN, column);
    }

    @Override
    public ColumnType type() {
      return column.type();
    }

    public io.stargate.db.schema.Column column() {
      return column;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind(), column);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Column)) return false;
      Column that = (Column) obj;
      return this.column.equals(that.column);
    }

    @Override
    public String toString() {
      return column.cqlName();
    }
  }

  // Note you can't modify nested fields in C*. Meaning, you can do `UPDATE .. SET c.foo = 3`, but
  // you cannot do `UPDATE .. SET c.foo.bar = 3` (even if `c.foo` is a UDT). So we keep this simple
  // here. IF C* generalize this, we'll need to generalize this as well, but it will be a bit more
  // annoying to use.
  public static class UdtField extends ModifiableEntity {
    private final String fieldName;
    private final ColumnType fieldType;

    private UdtField(io.stargate.db.schema.Column column, String fieldName) {
      super(Kind.UDT_FIELD, column);
      this.fieldName = fieldName;

      ColumnType type = column.type();
      if (type == null) {
        throw new IllegalArgumentException("Provided column does not have its type set");
      }
      if (!type.isUserDefined()) {
        throw new IllegalArgumentException(
            format(
                "Cannot access field %s of column %s of type %s, it is not a UDT",
                fieldName, column.cqlName(), type));
      }
      io.stargate.db.schema.Column field = ((UserDefinedType) type).columnMap().get(fieldName);
      if (field == null) {
        throw new IllegalArgumentException(
            format(
                "Cannot access undefined field %s of column %s of type %s",
                fieldName, column.cqlName(), type));
      }
      this.fieldType = field.type();
      if (this.fieldType == null) {
        throw new IllegalArgumentException(
            format(
                "The UDT type of %s is incomplete: it does not have a type for field %s",
                column.cqlName(), fieldName));
      }
    }

    @Override
    public ColumnType type() {
      return fieldType;
    }

    public String fieldName() {
      return fieldName;
    }

    public io.stargate.db.schema.Column parent() {
      return column;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind(), column, fieldName);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof UdtField)) return false;
      UdtField that = (UdtField) obj;
      return this.column.equals(that.column) && this.fieldName.equals(that.fieldName);
    }

    @Override
    public String toString() {
      return format("%s.%s", column.cqlName(), fieldName);
    }
  }

  // Same as with fields, you cannot nest map access. Meaning, you can do
  // `UPDATE .. SET c['foo'] = 3`, but not `UPDATE .. SET c['foo']['bar'] = 3` (even if `c.foo`
  // is a UDT). So we keep this simple here. IF C* generalize this, we'll need to generalize this as
  // well, but it will be a bit more annoying to use.
  public static class MapValue extends ModifiableEntity {
    private final TypedValue key;
    private final ColumnType valueType;

    private MapValue(io.stargate.db.schema.Column column, TypedValue key) {
      super(Kind.MAP_VALUE, column);
      this.key = key;

      ColumnType type = column.type();
      if (type == null) {
        throw new IllegalArgumentException("Provided column does not have its type set");
      }
      if (!type.isMap()) {
        throw new IllegalArgumentException(
            format(
                "Cannot access elements of column %s of type %s, it is not a map",
                column.cqlName(), type));
      }
      ColumnType keyType = type.parameters().get(0);
      // Checking ID is not truly sufficient, but unsure how reliable equals is on ColumnType, so
      // this is good enough for this. The server will do final validation anyway.
      if (keyType.id() != key.type().id()) {
        throw new IllegalArgumentException(
            format(
                "Cannot access value of map %s: expecting keys of type %s, but given a key of type %s",
                column.cqlName(), keyType, key.type()));
      }
      this.valueType = type.parameters().get(1);
    }

    @Override
    public ColumnType type() {
      return valueType;
    }

    public TypedValue key() {
      return key;
    }

    public io.stargate.db.schema.Column parent() {
      return column;
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind(), column, key);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MapValue)) return false;
      MapValue that = (MapValue) obj;
      return this.column.equals(that.column) && this.key.equals(that.key);
    }

    @Override
    public String toString() {
      return format("%s[%s]", column.cqlName(), key);
    }
  }
}
