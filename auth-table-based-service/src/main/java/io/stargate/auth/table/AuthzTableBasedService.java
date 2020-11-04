package io.stargate.auth.table;

import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Table;
import java.util.List;
import java.util.concurrent.Callable;

public class AuthzTableBasedService implements AuthorizationService {

  @Override
  public ResultSet authorizedDataRead(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    // Cannot perform authorization with a table based token so just return
    return action.call();
  }

  @Override
  public ResultSet authorizedDataWrite(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    // Cannot perform authorization with a table based token so just return
    return action.call();
  }

  @Override
  public ResultSet authorizedSchemaRead(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception {
    // Cannot perform authorization with a table based token so just return
    return action.call();
  }

  @Override
  public ResultSet authorizedSchemaWrite(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception {
    // Cannot perform authorization with a table based token so just return
    return action.call();
  }
}
