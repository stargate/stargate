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
package io.stargate.db.schema;

import static java.util.stream.Collectors.toList;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.UdtCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class UserDefinedType implements Column.ColumnType, SchemaEntity {

  private static final String ANONYMOUS = "<anonymous>";

  public abstract String keyspace();

  @Override
  public UserDefinedType frozen(boolean frozen) {
    return ImmutableUserDefinedType.copyOf(this).withIsFrozen(frozen);
  }

  public abstract List<Column> columns();

  @Value.Lazy
  public Map<String, Column> columnMap() {
    return columns().stream().collect(Collectors.toMap(Column::name, Function.identity()));
  }

  @Override
  public int id() {
    return rawType().id();
  }

  @Override
  public Column.Type rawType() {
    return Column.Type.UDT;
  }

  @Override
  public Class<?> javaType() {
    return UdtValue.class;
  }

  @Override
  public Object validate(Object value, String location) throws Column.ValidationException {
    checkKeyspaceSet();
    if (null != value) {
      if (!(value instanceof UdtValue)
          || !((UdtValue) value).getType().getName().asInternal().equals(name())) {
        throw new Column.ValidationException(value.getClass(), this, location);
      }
    }
    return value;
  }

  @Override
  public String cqlDefinition() {
    if (isFrozen()) {
      return "frozen<" + cqlName() + ">";
    }
    return cqlName();
  }

  @Override
  public UdtValue create(Object... fields) {
    checkKeyspaceSet();

    UdtValue udt = cqlDataType().newValue();
    Preconditions.checkArgument(
        fields.length == 0 || fields.length == columns().size(),
        "Expected %s parameter(s) when initializing '%s' but got %s",
        columns().size(),
        name(),
        fields.length);
    try {
      for (int count = 0; count < fields.length; count++) {
        Object value = fields[count];
        if (value != null) {
          Column.ColumnType type = columns().get(count).type();
          Object validated = type.validate(value, columns().get(count).name());
          udt = udt.set(count, validated, type.codec());
        }
      }
    } catch (Column.ValidationException e) {
      throw validationException(e);
    }
    return udt;
  }

  public IllegalArgumentException validationException(Column.ValidationException e) {
    return new IllegalArgumentException(
        String.format(
            "Wrong value type provided for user defined type '%s'. Provided type '%s' is not compatible with expected CQL type '%s' at location '%s'.",
            name(), e.providedType(), e.expectedCqlType(), e.location()));
  }

  public static UserDefinedType reference(String name) {
    return ImmutableUserDefinedType.builder().keyspace(ANONYMOUS).name(name).build();
  }

  public static UserDefinedType reference(String ksName, String name) {
    return ImmutableUserDefinedType.builder().keyspace(ksName).name(name).build();
  }

  @Override
  @Value.Lazy
  public TypeCodec codec() {
    checkKeyspaceSet();

    return new UdtCodec(cqlDataType());
  }

  private com.datastax.oss.driver.api.core.type.UserDefinedType cqlDataType() {
    return new DefaultUserDefinedType(
        CqlIdentifier.fromInternal(keyspace()),
        CqlIdentifier.fromInternal(name()),
        isFrozen(),
        columns().stream().map(c -> CqlIdentifier.fromInternal(c.name())).collect(toList()),
        columns().stream().map(c -> c.type().codec().getCqlType()).collect(toList()),
        CUSTOM_ATTACHMENT_POINT);
  }

  public void checkKeyspaceSet() {
    if (ANONYMOUS.equals(keyspace())) {
      throw new UnsupportedOperationException(
          "User defined type must be obtained from schema to be used");
    }
  }

  @Override
  public boolean isUserDefined() {
    return true;
  }

  @Override
  public UserDefinedType dereference(Keyspace keyspace) {
    if (!ANONYMOUS.equals(keyspace())) {
      return this;
    }
    UserDefinedType userDefinedType = keyspace.userDefinedType(name());
    Preconditions.checkArgument(
        userDefinedType != null,
        "User defined type '%s' does not exist in keyspace '%s'",
        name(),
        keyspace.name());
    return userDefinedType.frozen(isFrozen());
  }

  @Override
  public Column.ColumnType fieldType(String name) {
    Column column = columnMap().get(name);
    Preconditions.checkArgument(
        null != column, "User defined type '%s' does not have field '%s'", cqlName(), name);
    return column.type();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  public int schemaHashCode() {
    return SchemaHash.combine(
        SchemaHash.hashCode(isFrozen()),
        SchemaHash.hashCode(name()),
        SchemaHash.hashCode(keyspace()),
        SchemaHash.hash(columns()));
  }
}
