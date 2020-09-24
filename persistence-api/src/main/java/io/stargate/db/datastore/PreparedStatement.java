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
package io.stargate.db.datastore;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.datastore.query.WhereCondition;
import io.stargate.db.datastore.schema.AbstractTable;
import io.stargate.db.datastore.schema.Column;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Represents a prepared statement which either maps to the internal or the DSE driver's {@link
 * com.datastax.oss.driver.api.core.cql.PreparedStatement}.
 */
public interface PreparedStatement {
  default CompletableFuture<ResultSet> execute(DataStore dataStore, Object... parameters) {
    return execute(dataStore, Optional.empty(), parameters);
  }

  CompletableFuture<ResultSet> execute(
      DataStore dataStore, Optional<ConsistencyLevel> cl, Object... parameters);

  String IN = "in(";
  String KEY = "key(";
  String VALUE = "value(";

  int IN_LEN = IN.length();
  int KEY_LEN = KEY.length();
  int VALUE_LEN = VALUE.length();

  default Object validateParameter(AbstractTable table, String columnName, Object parameter) {
    return validateParameter(table, columnName, Optional.empty(), parameter);
  }

  default Object validateParameter(
      AbstractTable table, String columnName, Optional<String[]> path, Object parameter) {
    Column column = null;
    Column.ColumnType type;
    if (WhereCondition.NULL.equals(parameter) || WhereCondition.UNSET.equals(parameter)) {
      column = table.column(columnName);
      Preconditions.checkArgument(
          !column.isPartitionKey(),
          "'%s' value provided for column '%s'. Partition keys must have a value.",
          parameter,
          column.name());
      return parameter;
    }
    try {
      if (columnName.regionMatches(true, 0, IN, 0, IN_LEN)) {
        column = table.column(columnName.substring(IN.length(), columnName.length() - 1));
        type = column.type();
        List list = (List) parameter;
        List validated = new ArrayList(list.size());

        for (Object o : list) {
          validated.add(type.validate(o, columnName));
        }
        return validated;
      } else if (columnName.regionMatches(true, 0, KEY, 0, KEY_LEN)) {
        column = table.column(columnName.substring(KEY.length(), columnName.length() - 1));
        type = column.type().parameters().get(0);
        return type.validate(parameter, columnName);
      } else if (columnName.regionMatches(true, 0, VALUE, 0, VALUE_LEN)) {
        column = table.column(columnName.substring(VALUE.length(), columnName.length() - 1));

        type = column.type().parameters().get(column.type().rawType() == Column.Type.Map ? 1 : 0);
        return type.validate(parameter, columnName);
      } else {
        column = table.column(columnName);
        if (null != column) {
          type = column.type();
          return type.validate(parameter, column.name());
        }
      }
    } catch (Column.ValidationException e) {
      IllegalArgumentException iae;
      if (column.type().isComplexType()) {
        iae =
            new IllegalArgumentException(
                String.format(
                    "Wrong value type provided for column '%s'. Provided type '%s' is not compatible with expected CQL type '%s' at location '%s'.%s",
                    column.name(),
                    e.providedType(),
                    e.expectedCqlType(),
                    e.location(),
                    e.errorDetails()));
      } else {
        iae =
            new IllegalArgumentException(
                String.format(
                    "Wrong value type provided for column '%s'. Provided type '%s' is not compatible with expected CQL type '%s'.%s",
                    column.name(),
                    parameter.getClass().getSimpleName(),
                    e.expectedCqlType(),
                    e.errorDetails()));
      }

      iae.addSuppressed(e);
      throw iae;
    }
    return parameter;
  }
}
