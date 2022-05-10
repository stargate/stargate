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

import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

public interface AuthorizationService {

  /**
   * Using the provided token will perform pre-authorization where possible, executes the query
   * provided, and then authorizes the response of the query.
   *
   * @param action The action to be executed and authorized against a token.
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspace The keyspace containing the table with data to be read.
   * @param table The table within the provided keyspace containing the data to be read.
   * @param typedKeyValues A list of {@link TypedKeyValue} that will be used in the query and should
   *     be authorized against the token.
   * @param sourceAPI The source api which calls this method.
   * @return On success will return the result of the query and otherwise will return an exception
   *     relating to the failure to authorize.
   * @throws Exception An exception relating to the failure to authorize.
   */
  ResultSet authorizedDataRead(
      Callable<ResultSet> action,
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      SourceAPI sourceAPI)
      throws Exception;

  /**
   * Using the provided token will perform pre-authorization and if not successful throws an
   * exception. Intended to be used when the keys for the query are not readily accessible or when a
   * higher level of authorization is acceptable.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspace The keyspace containing the table with data to be read.
   * @param table The table within the provided keyspace containing the data to be read.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeDataRead(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization and if not successful throws an
   * exception. Intended to be used when the keys for the query are not readily accessible or when a
   * higher level of authorization is acceptable.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspace Either the keyspace containing the resource to be modified or the actual
   *     resource being modified.
   * @param table The table within the provided keyspace containing the data to be modified.
   * @param scope The table within the provided keyspace that is being modified.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeDataWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization where possible.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param typedKeyValues A list of {@link TypedKeyValue} that will be used in the query and should
   *     be authorized against the token.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeDataWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization of accessing the provided resources.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspaceNames Either the keyspace(s) containing the resource(s) to be read or the actual
   *     resource being read.
   * @param tableNames The table(s) within the provided keyspace(s) that is being read.
   * @param sourceAPI The source api which calls this method.
   * @param resource The resource for which read authorization is being requested.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeSchemaRead(
      AuthenticationSubject authenticationSubject,
      List<String> keyspaceNames,
      List<String> tableNames,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization where possible and if not successful
   * throws an exception.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspace Either the keyspace containing the resource to be modified or the actual
   *     resource being modified.
   * @param table The table within the provided keyspace that is being modified.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @param resource The resource for which read authorization is being requested.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeSchemaWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      Scope scope,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization where possible and if not successful
   * throws an exception.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param keyspace Either the keyspace containing the resource to be modified or the actual
   *     resource being modified.
   * @param table The table within the provided keyspace that is being modified.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @param resource The resource for which read authorization is being requested.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  default void authorizeAdvancedWorkloadSchemaWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      Scope scope,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException {
    throw new UnauthorizedException(
        String.format(
            "Not authorized to access %s.%s for %s (from source api: %s)",
            keyspace, table, scope.name(), sourceAPI.getName()));
  }

  /**
   * Using the provided token will perform pre-authorization of role management.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param role The role which is being modified.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeRoleManagement(
      AuthenticationSubject authenticationSubject, String role, Scope scope, SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization of role management.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param role The role containing all of the permissions to be given to the grantee.
   * @param grantee The role that is being granted or revoked the role.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeRoleManagement(
      AuthenticationSubject authenticationSubject,
      String role,
      String grantee,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization of role access.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param role The role that is being accessed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizeRoleRead(
      AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization of permission management.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param resource The resource that the grantee is being given permissions to.
   * @param grantee The role that is being granted access to the resource.
   * @param scope The {@link Scope} of the action to be performed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizePermissionManagement(
      AuthenticationSubject authenticationSubject,
      String resource,
      String grantee,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException;

  /**
   * Using the provided token will perform pre-authorization of permission access.
   *
   * @param authenticationSubject The authenticated user to use for authorization.
   * @param role The role for which the permissions are being accessed.
   * @param sourceAPI The source api which calls this method.
   * @throws UnauthorizedException An exception relating to the failure to authorize.
   */
  void authorizePermissionRead(
      AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
      throws UnauthorizedException;
}
