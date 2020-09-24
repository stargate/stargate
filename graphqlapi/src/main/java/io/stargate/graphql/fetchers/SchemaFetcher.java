package io.stargate.graphql.fetchers;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public interface SchemaFetcher extends DataFetcher {
  String getQuery(DataFetchingEnvironment dataFetchingEnvironment);

  default DataType decodeType(Object typeObject) {
    Map<String, Object> type = (Map<String, Object>) typeObject;
    String basic = (String) type.get("basic");
    Map<String, Object> info = (Map<String, Object>) type.get("info");

    switch (basic) {
      case "INT":
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
      case "INET":
        return DataTypes.INT;
      case "ASCII":
        return DataTypes.ASCII;
      case "DECIMAL":
        return DataTypes.DECIMAL;
      case "BLOB":
        return DataTypes.BLOB;
      case "VARCHAR":
        return DataTypes.TEXT;
      case "DOUBLE":
        return DataTypes.DOUBLE;
      case "COUNTER":
        return DataTypes.COUNTER;
      case "DATE":
        return DataTypes.DATE;
      case "TEXT":
        return DataTypes.TEXT;
      case "FLOAT":
        return DataTypes.FLOAT;
      case "CUSTOM":
        return null;
      case "LIST":
        return DataTypes.listOf(decodeType(info.get("subTypes")));
      case "SET":
        return DataTypes.setOf(decodeType(info.get("subTypes")));
      case "MAP":
        return null;
      case "TUPLE":
        return null;
      case "UDT":
        return null;
    }
    return null;
  }
}
