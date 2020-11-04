package io.stargate.auth;

import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.concurrent.Callable;

public interface AuthorizationService {

  ResultSet authorizedDataRead(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception;

  ResultSet authorizedDataWrite(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception;

  ResultSet authorizedSchemaRead(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception;

  ResultSet authorizedSchemaWrite(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception;
}
