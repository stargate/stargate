package io.stargate.auth;

import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.WhereCondition;

public class TypedKeyValue {

  String name;
  String type;
  Object value;

  public TypedKeyValue(WhereCondition<?> w) {
    this.name = w.column().name();
    if (w.column() != null && w.column().type() != null) {
      //noinspection ConstantConditions
      this.type = w.column().type().cqlDefinition();
    }

    this.value = w.value().isPresent() ? w.value().get() : null;
  }

  public TypedKeyValue(Value<?> v) {
    this.name = v.column().name();
    if (v.column() != null && v.column().type() != null) {
      //noinspection ConstantConditions
      this.type = v.column().type().cqlDefinition();
    }

    this.value = v.value().isPresent() ? v.value().get() : null;
  }

  public TypedKeyValue(String name, String type, Object value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public TypedKeyValue setName(String name) {
    this.name = name;
    return this;
  }

  public String getType() {
    return type;
  }

  public TypedKeyValue setType(String type) {
    this.type = type;
    return this;
  }

  public Object getValue() {
    return value;
  }

  public TypedKeyValue setValue(Object value) {
    this.value = value;
    return this;
  }
}
