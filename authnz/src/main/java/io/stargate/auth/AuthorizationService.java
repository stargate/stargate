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
package io.stargate.auth;

import io.stargate.db.datastore.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

public interface AuthorizationService {

  /**
   * Using the provided token will perform pre-authorization where possible, executes the query
   * provided, and then authorizes the response of the query.
   *
   * @param action A {@link QueryBuilder} object to be executed and authorized against a token.
   * @param token The authenticated token to use for authorization.
   * @param typedKeyValues A list of {@link TypedKeyValue} that will be used in the query and should
   *     be authorized against the token.
   * @return On success will return the result of the query and otherwise will return an exception
   *     relating to the failure to authorize.
   * @throws Exception An exception relating to the failure to authorize.
   */
  ResultSet authorizedDataRead(
      Callable<ResultSet> action, String token, List<TypedKeyValue> typedKeyValues)
      throws Exception;

  /**
   * Using the provided token will perform pre-authorization where possible and if successful
   * executes the query provided.
   *
   * @param action A {@link QueryBuilder} object to be executed and authorized against a token.
   * @param token The authenticated token to use for authorization.
   * @param typedKeyValues A list of {@link TypedKeyValue} that will be used in the query and should
   *     be authorized against the token.
   * @return On success will return the result of the query and otherwise will return an exception
   *     relating to the failure to authorize.
   * @throws Exception An exception relating to the failure to authorize.
   */
  ResultSet authorizedDataWrite(
      Callable<ResultSet> action, String token, List<TypedKeyValue> typedKeyValues)
      throws Exception;

  /**
   * Using the provided token will perform pre-authorization of accessing the provided resources.
   *
   * @param token The authenticated token to use for authorization.
   * @param keyspaceNames Either the keyspace(s) containing the resource(s) to be read or the actual
   *     resource being read.
   * @param tableNames The table(s) within the provided keyspace(s) that is being read.
   * @throws Exception An exception relating to the failure to authorize.
   */
  void authorizeSchemaRead(String token, List<String> keyspaceNames, List<String> tableNames)
      throws Exception;

  /**
   * Using the provided token will perform pre-authorization where possible and if successful
   * executes the query provided.
   *
   * @param action A {@link QueryBuilder} object to be executed and authorized against a token.
   * @param token The authenticated token to use for authorization.
   * @param keyspace Either the keyspace containing the resource to be modified or the actual
   *     resource being modified.
   * @param table The table within the provided keyspace that is being modified.
   * @return On success will return the result of the query and otherwise will return an exception
   *     relating to the failure to authorize.
   * @throws Exception An exception relating to the failure to authorize.
   */
  ResultSet authorizedSchemaWrite(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception;
}
