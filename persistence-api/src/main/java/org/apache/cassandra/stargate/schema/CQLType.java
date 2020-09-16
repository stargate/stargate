package org.apache.cassandra.stargate.schema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;

public interface CQLType {
  default boolean isCollection() {
    return false;
  }

  default boolean isUDT() {
    return false;
  }

  public enum Native implements CQLType {
    ASCII,
    BIGINT,
    BLOB,
    BOOLEAN,
    COUNTER,
    DATE,
    DECIMAL,
    DOUBLE,
    DURATION,
    FLOAT,
    INET,
    INT,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TIMEUUID,
    TINYINT,
    UUID,
    VARCHAR,
    VARINT;

    Native() {}

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  public static class Custom implements CQLType {
    private final String className;

    public Custom(String className) {
      this.className = className;
    }

    @Override
    public final boolean equals(Object o) {
      if (!(o instanceof Custom)) return false;

      Custom that = (Custom) o;
      return this.className.equals(that.className);
    }

    @Override
    public final int hashCode() {
      return className.hashCode();
    }

    @Override
    public String toString() {
      return "'" + className + '\'';
    }
  }

  /** Represents a list or a set data type. */
  public static class Collection implements CQLType {
    private final Kind kind;
    private final CQLType subType;

    public enum Kind {
      LIST,
      SET
    }

    public Collection(Kind kind, CQLType subType) {
      this.kind = kind;
      this.subType = subType;
    }

    public boolean isCollection() {
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Collection that = (Collection) o;
      return kind == that.kind && subType.equals(that.subType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, subType);
    }

    @Override
    public String toString() {
      return String.format("%s<%s>", kind.toString().toLowerCase(), subType);
    }
  }

  public static class MapDataType implements CQLType {
    private final CQLType keyType;
    private final CQLType valueType;

    @Override
    public boolean isCollection() {
      return true;
    }

    public MapDataType(CQLType keyType, CQLType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MapDataType that = (MapDataType) o;
      return keyType.equals(that.keyType) && valueType.equals(that.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyType, valueType);
    }

    @Override
    public String toString() {
      return String.format("map<%s, %s>", keyType, valueType);
    }
  }

  public static class UserDefined implements CQLType {
    private final String keyspace;
    private final String name;
    private final LinkedHashMap<String, CQLType> fields;

    public UserDefined(String keyspace, String name, LinkedHashMap<String, CQLType> fields) {
      this.keyspace = keyspace;
      this.name = name;
      this.fields = fields;
    }

    @Override
    public boolean isUDT() {
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UserDefined that = (UserDefined) o;
      return keyspace.equals(that.keyspace) && name.equals(that.name) && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyspace, name, fields);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static class Tuple implements CQLType {
    private final CQLType[] subTypes;

    public Tuple(CQLType... subTypes) {
      this.subTypes = subTypes;

      if (subTypes == null || subTypes.length == 0) {
        throw new IllegalArgumentException("subTypes should contain at least an item");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tuple tuple = (Tuple) o;
      return Arrays.equals(subTypes, tuple.subTypes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(subTypes);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("tuple<");
      builder.append(subTypes[0].toString());
      for (int i = 1; i < subTypes.length; i++) {
        builder.append(", ");
        builder.append(subTypes[i].toString());
      }
      builder.append(">");
      return builder.toString();
    }
  }
}
