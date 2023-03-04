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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.codec.ListCodec;
import com.datastax.oss.driver.internal.core.type.codec.MapCodec;
import com.datastax.oss.driver.internal.core.type.codec.SetCodec;
import com.datastax.oss.driver.internal.core.type.codec.TupleCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public abstract class ParameterizedType implements Column.ColumnType {

  @Override
  public abstract List<Column.ColumnType> parameters();

  @Override
  public int id() {
    return rawType().id();
  }

  @Override
  public String name() {
    throw new UnsupportedOperationException("Parameterized types do not have a name");
  }

  @Override
  public String cqlDefinition() {
    String definition =
        rawType().cqlDefinition()
            + parameters().stream()
                .map(p -> p.cqlDefinition())
                .collect(Collectors.joining(", ", "<", ">"));
    if (isFrozen()) {
      definition = "frozen<" + definition + ">";
    }
    return definition;
  }

  @Override
  public boolean isParameterized() {
    return true;
  }

  @Override
  public Class<?> javaType() {
    return rawType().javaType();
  }

  protected abstract ParameterizedType withIsFrozen(boolean value);

  @Override
  public Column.ColumnType frozen(boolean frozen) {
    return withIsFrozen(frozen);
  }

  public interface Builder {
    Builder addParameters(Column.ColumnType element);

    ParameterizedType build();
  }

  @Value.Immutable(prehash = true)
  public abstract static class ListType extends ParameterizedType {

    @SuppressWarnings("unchecked")
    @Override
    public Object validate(Object value, String location) throws Column.ValidationException {
      value = super.validate(value, location);

      if (value != null) {
        List list = (List) value;
        List validated = new ArrayList(list.size());
        Column.ColumnType type = parameters().get(0);
        for (Object o : list) {
          Object validatedValue = type.validate(o, cqlDefinition());
          validated.add(validatedValue);
        }
        return validated;
      }
      return value;
    }

    @Override
    public List<?> create(Object... entries) {
      return new ArrayList<>(Arrays.asList(entries));
    }

    @Override
    public TypeCodec codec() {
      TypeCodec elementCodec = parameters().get(0).codec();
      return new ListCodec<>(DataTypes.listOf(elementCodec.getCqlType(), isFrozen()), elementCodec);
    }

    @Override
    public boolean isCollection() {
      return true;
    }

    @Override
    public boolean isList() {
      return true;
    }

    @Override
    public Column.Type rawType() {
      return Column.Type.List;
    }

    @Override
    public Column.ColumnType dereference(Keyspace keyspace) {
      return ImmutableListType.copyOf(this)
          .withParameters(
              parameters().stream().map(t -> t.dereference(keyspace)).collect(Collectors.toList()));
    }

    public abstract static class Builder implements ParameterizedType.Builder {}

    @Override
    public Column.ColumnType fieldType(String name) {
      return parameters().get(0);
    }

    @Override
    @Value.Derived
    @Value.Auxiliary
    public int schemaHashCode() {
      return SchemaHashable.combine(
          SchemaHashable.hashCode(isFrozen()), SchemaHashable.hashCode(parameters()));
    }
  }

  @Value.Immutable(prehash = true)
  public abstract static class SetType extends ParameterizedType {

    @SuppressWarnings("unchecked")
    @Override
    public Object validate(Object value, String location) throws Column.ValidationException {
      value = super.validate(value, location);
      if (value != null) {
        Collection validated;
        Set set = (Set) value;
        try {
          validated = set.getClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          validated = new HashSet<>(set.size());
        }
        Column.ColumnType type = parameters().get(0);
        for (Object o : set) {
          Object validatedValue = type.validate(o, cqlDefinition());
          validated.add(validatedValue);
        }
        return validated;
      }
      return value;
    }

    @Override
    public Set<?> create(Object... entries) {
      return new LinkedHashSet<>(Arrays.asList(entries));
    }

    @Override
    public TypeCodec codec() {
      TypeCodec elementCodec = parameters().get(0).codec();
      return new SetCodec<>(DataTypes.setOf(elementCodec.getCqlType(), isFrozen()), elementCodec);
    }

    @Override
    public boolean isCollection() {
      return true;
    }

    @Override
    public boolean isSet() {
      return true;
    }

    @Override
    public Column.Type rawType() {
      return Column.Type.Set;
    }

    @Override
    public Column.ColumnType dereference(Keyspace keyspace) {
      return ImmutableSetType.copyOf(this)
          .withParameters(
              parameters().stream().map(t -> t.dereference(keyspace)).collect(Collectors.toList()));
    }

    public abstract static class Builder implements ParameterizedType.Builder {}

    @Override
    public Column.ColumnType fieldType(String name) {
      return parameters().get(0);
    }

    @Override
    @Value.Derived
    @Value.Auxiliary
    public int schemaHashCode() {
      return SchemaHashable.combine(
          SchemaHashable.hashCode(isFrozen()), SchemaHashable.hashCode(parameters()));
    }
  }

  @Value.Immutable(prehash = true)
  public abstract static class MapType extends ParameterizedType {
    @SuppressWarnings("unchecked")
    @Override
    public Object validate(Object value, String location) throws Column.ValidationException {
      value = super.validate(value, location);
      if (value != null) {
        Map validated;
        Map<?, ?> map = (Map<?, ?>) value;
        try {
          validated = map.getClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          validated = new HashMap(map.size());
        }
        Column.ColumnType keyType = parameters().get(0);
        Column.ColumnType valueType = parameters().get(1);
        for (Map.Entry<?, ?> o : map.entrySet()) {
          Object key = keyType.validate(o.getKey(), cqlDefinition() + "[key]");
          Object val = valueType.validate(o.getValue(), cqlDefinition() + "[value]");
          validated.put(key, val);
        }
        return validated;
      }
      return value;
    }

    @Override
    public Map<?, ?> create(Object... args) {
      Preconditions.checkArgument(
          null != args,
          "Your arguments were null. If you want to create an empty map, just call .create() with no parameters.");
      Preconditions.checkArgument(
          (args.length % 2) == 0,
          "Please provide an even number of parameters. These are interpreted as key/value pairs to the map.");

      final LinkedHashMap<Object, Object> map = new LinkedHashMap<>();

      for (int i = 0; i < args.length; i += 2) {
        int j = i + 1;
        map.put(args[i], args[j]);
      }

      return map;
    }

    @Override
    public TypeCodec codec() {
      TypeCodec keyCodec = parameters().get(0).codec();
      TypeCodec valueCodec = parameters().get(1).codec();
      return new MapCodec<>(
          DataTypes.mapOf(keyCodec.getCqlType(), valueCodec.getCqlType(), isFrozen()),
          keyCodec,
          valueCodec);
    }

    @Override
    public boolean isCollection() {
      return true;
    }

    @Override
    public boolean isMap() {
      return true;
    }

    @Override
    public Column.Type rawType() {
      return Column.Type.Map;
    }

    @Override
    public Column.ColumnType dereference(Keyspace keyspace) {
      return ImmutableMapType.copyOf(this)
          .withParameters(
              parameters().stream().map(t -> t.dereference(keyspace)).collect(Collectors.toList()));
    }

    public abstract static class Builder implements ParameterizedType.Builder {}

    @Override
    public Column.ColumnType fieldType(String name) {
      throw new UnsupportedOperationException("Map type does not support nested access");
    }

    @Override
    @Value.Derived
    @Value.Auxiliary
    public int schemaHashCode() {
      return SchemaHashable.combine(
          SchemaHashable.hashCode(isFrozen()), SchemaHashable.hashCode(parameters()));
    }
  }

  @Value.Immutable(prehash = true)
  public abstract static class TupleType extends ParameterizedType {
    @Override
    public Object validate(Object value, String location) throws Column.ValidationException {
      value = super.validate(value, location);

      if (null != value) {
        TupleValue tupleValue = (TupleValue) value;
        for (int count = 0; count < parameters().size(); count++) {
          Column.ColumnType type = parameters().get(count);
          DataType dataType = tupleValue.getType().getComponentTypes().get(count);
          String fieldLocation = location + "." + cqlDefinition() + "[" + count + "]";
          boolean compatible = type.codec().accepts(dataType);
          Object o;
          if (type.isComplexType() || !compatible) {
            o = tupleValue.getObject(count);
          } else {
            try {
              o = tupleValue.get(count, type.codec());
            } catch (RuntimeException e) {
              throw new Column.ValidationException(this, fieldLocation);
            }
          }
          Object validated = type.validate(o, fieldLocation);

          tupleValue = tupleValue.set(count, validated, type.codec());
        }
      }
      return value;
    }

    @Override
    public boolean isFrozen() {
      return true;
    }

    @Override
    public TupleValue create(Object... parameters) {
      return populate(cqlDataType().newValue(), parameters);
    }

    public TupleValue populate(TupleValue tuple, Object[] parameters) {
      try {
        for (int count = 0; count < parameters.length; count++) {
          Column.ColumnType columnType = parameters().get(count);
          Object validated = columnType.validate(parameters[count], "[" + count + "]");
          tuple = tuple.set(count, validated, columnType.codec());
        }
      } catch (Column.ValidationException e) {
        throw new IllegalArgumentException(
            String.format(
                "Wrong value type provided for tuple '%s'. Provided type '%s' is not compatible with expected CQL type '%s' at location '%s'.",
                cqlDefinition(), e.providedType(), e.expectedCqlType(), e.location()));
      }
      return tuple;
    }

    @Override
    public TypeCodec codec() {
      return new TupleCodec(cqlDataType());
    }

    private com.datastax.oss.driver.api.core.type.TupleType cqlDataType() {
      return new DefaultTupleType(
          parameters().stream().map(p -> p.codec().getCqlType()).collect(toList()),
          CUSTOM_ATTACHMENT_POINT);
    }

    @Override
    public boolean isCollection() {
      return false;
    }

    @Override
    public Column.Type rawType() {
      return Column.Type.Tuple;
    }

    @Override
    public Column.ColumnType dereference(Keyspace keyspace) {
      return ImmutableTupleType.copyOf(this)
          .withParameters(
              parameters().stream().map(t -> t.dereference(keyspace)).collect(Collectors.toList()));
    }

    @Override
    public String cqlDefinition() {
      String definition =
          rawType().cqlDefinition()
              + parameters().stream()
                  .map(p -> p.cqlDefinition())
                  .collect(Collectors.joining(", ", "<", ">"));
      if (isFrozen()) {
        definition = "frozen<" + definition + ">";
      }
      return definition;
    }

    @Override
    public boolean isTuple() {
      return true;
    }

    @Override
    protected TupleType withIsFrozen(boolean value) {
      return this;
    }

    public abstract static class Builder implements ParameterizedType.Builder {}

    @Value.Lazy
    public Map<String, Column.ColumnType> parameterMap() {
      int count = 1; // Tuples have 1 based fields
      Map<String, Column.ColumnType> parameterMap = new HashMap<>();
      for (Column.ColumnType parameter : parameters()) {
        parameterMap.put("field" + count++, parameter);
      }
      return parameterMap;
    }

    @Override
    public Column.ColumnType fieldType(String name) {
      Column.ColumnType parameterType = parameterMap().get(name);
      Preconditions.checkArgument(
          null != parameterType, "Tuple type '%s' does not have field '%s'", cqlDefinition(), name);
      return parameterType;
    }

    @Override
    @Value.Derived
    @Value.Auxiliary
    public int schemaHashCode() {
      return SchemaHashable.hash(parameters());
    }
  }
}
