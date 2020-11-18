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
package io.stargate.auth.table;

import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

public class AuthzTableBasedService implements AuthorizationService {

  /**
   * Authorization for data access is not provided by table based tokens so all authorization will
   * be deferred to the underlying permissions assigned to the role the token maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public ResultSet authorizedDataRead(
      Callable<ResultSet> action, String token, List<TypedKeyValue> typedKeyValues)
      throws Exception {
    // Cannot perform authorization with a table based token so just return
    return action.call();
  }

  /**
   * Authorization for data access is not provided by table based tokens so all authorization will
   * be deferred to the underlying permissions assigned to the role the token maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeDataRead(String token, String keyspaceNames, String tableNames)
      throws UnauthorizedException {
    // Cannot perform authorization with a table based token so just return
  }

  /**
   * Authorization for data access is not provided by table based tokens so all authorization will
   * be deferred to the underlying permissions assigned to the role the token maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeDataWrite(String token, String keyspaceNames, String tableNames, Scope scope)
      throws UnauthorizedException {
    // Cannot perform authorization with a table based token so just return
  }

  /**
   * Authorization for data access is not provided by table based tokens so all authorization will
   * be deferred to the underlying permissions assigned to the role the token maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizedDataWrite(String token, List<TypedKeyValue> typedKeyValues, Scope scope)
      throws UnauthorizedException {
    // Cannot perform authorization with a table based token so just return
  }

  /**
   * Authorization for schema resource access is not provided by table based tokens so all
   * authorization will be deferred to the underlying permissions assigned to the role the token
   * maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeSchemaRead(String token, List<String> keyspaceNames, List<String> tableNames)
      throws Exception {
    // Cannot perform authorization with a table based token so just return
  }

  /**
   * Authorization for schema resource access is not provided by table based tokens so all
   * authorization will be deferred to the underlying permissions assigned to the role the token
   * maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeSchemaWrite(String token, String keyspace, String table, Scope scope)
      throws UnauthorizedException {
    // Cannot perform authorization with a table based token so just return
  }
}
