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
package io.stargate.graphql.schema.fetchers.ddl;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.List;
import java.util.Map;

/**
 * Base class for fetchers that execute a single DDL query, such as a CREATE KEYSPACE or DROP TABLE.
 */
public abstract class DdlQueryFetcher extends CassandraFetcher<Boolean> {

  protected DdlQueryFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected Boolean get(DataFetchingEnvironment environment, DataStore dataStore) throws Exception {
    dataStore.query(getQuery(environment)).get();
    return true;
  }

  abstract String getQuery(DataFetchingEnvironment dataFetchingEnvironment)
      throws UnauthorizedException;

  protected DataType decodeType(Object typeObject) {
    // TODO can these casts fail? If so add proper error handling.
    @SuppressWarnings("unchecked")
    Map<String, Object> type = (Map<String, Object>) typeObject;
    String basic = (String) type.get("basic");
    @SuppressWarnings("unchecked")
    Map<String, Object> info = (Map<String, Object>) type.get("info");
    List<?> subTypes;

    switch (basic) {
      case "INT":
      case "INET":
        return DataTypes.INT;
      case "TIMEUUID":
        return DataTypes.TIMEUUID;
      case "TIMESTAMP":
        return DataTypes.TIMESTAMP;
      case "BIGINT":
        return DataTypes.BIGINT;
      case "TIME":
        return DataTypes.TIME;
      case "DURATION":
        return DataTypes.DURATION;
      case "VARINT":
        return DataTypes.VARINT;
      case "UUID":
        return DataTypes.UUID;
      case "BOOLEAN":
        return DataTypes.BOOLEAN;
      case "TINYINT":
        return DataTypes.TINYINT;
      case "SMALLINT":
        return DataTypes.SMALLINT;
      case "ASCII":
        return DataTypes.ASCII;
      case "DECIMAL":
        return DataTypes.DECIMAL;
      case "BLOB":
        return DataTypes.BLOB;
      case "VARCHAR":
      case "TEXT":
        return DataTypes.TEXT;
      case "DOUBLE":
        return DataTypes.DOUBLE;
      case "COUNTER":
        return DataTypes.COUNTER;
      case "DATE":
        return DataTypes.DATE;
      case "FLOAT":
        return DataTypes.FLOAT;
      case "LIST":
        subTypes = (List<?>) info.get("subTypes");
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("List sub types should contain 1 item");
        }
        return DataTypes.listOf(decodeType(subTypes.get(0)));
      case "SET":
        subTypes = (List<?>) info.get("subTypes");
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("Set sub types should contain 1 item");
        }
        subTypes = (List<?>) info.get("subTypes");
        return DataTypes.setOf(decodeType(subTypes.get(0)));
      case "MAP":
        subTypes = (List<?>) info.get("subTypes");
        if (subTypes == null || subTypes.size() != 2) {
          throw new IllegalArgumentException("Map sub types should contain 2 items");
        }
        return DataTypes.mapOf(decodeType(subTypes.get(0)), decodeType(subTypes.get(1)));
    }

    throw new RuntimeException(String.format("Data type %s is not supported", basic));
  }
}
