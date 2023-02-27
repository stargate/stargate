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
package io.stargate.auth.jwt;

import static io.stargate.auth.jwt.AuthnJwtService.CLAIMS_FIELD;
import static io.stargate.auth.jwt.AuthnJwtService.STARGATE_PREFIX;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthzJwtService implements AuthorizationService {

  private static final Logger log = LoggerFactory.getLogger(AuthzJwtService.class);
  private final Pattern tokenPattern = Pattern.compile("\\.");

  /**
   * Using the provided JWT and the claims it contains will perform pre-authorization where
   * possible, executes the query provided, and then authorizes the response of the query.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public ResultSet authorizedDataRead(
      Callable<ResultSet> action,
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      SourceAPI
          sourceAPI) // this isn’t supported but if you want to use it you’ll need something other
      // than a JWT
      throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(authenticationSubject.token());

    preCheckDataReadWrite(stargateClaims, typedKeyValues);

    ResultSet result = action.call();

    if (result == null) {
      return null;
    }

    return result.withRowInspector(row -> hasCorrectClaims(stargateClaims, row));
  }

  static boolean hasCorrectClaims(JSONObject stargateClaims, io.stargate.db.datastore.Row row) {
    if (row == null) {
      return true;
    }

    for (Column col : row.columns()) {
      if (stargateClaims.has(STARGATE_PREFIX + col.name())) {

        String stargateClaimValue;
        try {
          stargateClaimValue = stargateClaims.getString(STARGATE_PREFIX + col.name());
        } catch (JSONException e) {
          log.warn("Unable to get stargate claim for " + STARGATE_PREFIX + col.name());
          return false;
        }
        String columnValue = row.getString(col.name());
        if (!stargateClaimValue.equals(columnValue)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Authorization for data resource access without keys is not provided by JWTs so all
   * authorization will be deferred to the underlying permissions assigned to the role the JWT maps
   * to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeDataRead(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for data resource access without keys is not provided by JWTs so all
   * authorization will be deferred to the underlying permissions assigned to the role the JWT maps
   * to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeDataWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /** {@inheritdoc} */
  @Override
  public void authorizeDataWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    JSONObject stargateClaims = extractClaimsFromJWT(authenticationSubject.token());

    preCheckDataReadWrite(stargateClaims, typedKeyValues);

    // Just return. No value in doing a post check since we can't roll back anyway.
  }

  /**
   * Authorization for schema resource access is not provided by JWTs so all authorization will be
   * deferred to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeSchemaRead(
      AuthenticationSubject authenticationSubject,
      List<String> keyspaceNames,
      List<String> tableNames,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for schema resource access is not provided by JWTs so all authorization will be
   * deferred to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeSchemaWrite(
      AuthenticationSubject authenticationSubject,
      String keyspace,
      String table,
      Scope scope,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for role management is not provided by JWTs so all authorization will be deferred
   * to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeRoleManagement(
      AuthenticationSubject authenticationSubject, String role, Scope scope, SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for role management is not provided by JWTs so all authorization will be deferred
   * to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeRoleManagement(
      AuthenticationSubject authenticationSubject,
      String role,
      String grantee,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for role management is not provided by JWTs so all authorization will be deferred
   * to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizeRoleRead(
      AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for permission management is not provided by JWTs so all authorization will be
   * deferred to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizePermissionManagement(
      AuthenticationSubject authenticationSubject,
      String resource,
      String grantee,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  /**
   * Authorization for permission management is not provided by JWTs so all authorization will be
   * deferred to the underlying permissions assigned to the role the JWT maps to.
   *
   * <p>{@inheritdoc}
   */
  @Override
  public void authorizePermissionRead(
      AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
      throws UnauthorizedException {
    // Cannot perform authorization with a JWT token so just return
  }

  private JSONObject extractClaimsFromJWT(String token) throws JSONException {
    // Grab the custom claims from the JWT. It's safe to work with the JWT as a plain Base64 encoded
    // json object here since by this point we've already authenticated the request.
    String[] parts = tokenPattern.split(token);
    if (parts.length < 3) {
      throw new IllegalArgumentException(
          "Valid JWT should contain 3 parts but provided only contains " + parts.length);
    }

    String decodedPayload =
        new String(
            Base64.getUrlDecoder().decode(parts[1]),
            // Per RFC-7519, JWTs are encoded from the UTF-8 representation of the JSON payload:
            StandardCharsets.UTF_8);
    JSONObject payload = new JSONObject(decodedPayload);
    return payload.getJSONObject(CLAIMS_FIELD);
  }

  private void preCheckDataReadWrite(JSONObject stargateClaims, List<TypedKeyValue> typedKeyValues)
      throws JSONException, UnauthorizedException {
    for (TypedKeyValue typedKeyValue : typedKeyValues) {
      // If one of the columns exist as a field in the JWT claims and the values do not match then
      // the request is not allowed.
      if (stargateClaims.has(STARGATE_PREFIX + typedKeyValue.getName())) {
        ColumnType targetCellType = typedKeyValue.getType();
        if (!targetCellType.equals(Type.Text)) {
          throw new IllegalArgumentException(
              "Column must be of type text to be used for authorization");
        }

        String stargateClaimValue =
            stargateClaims.getString(STARGATE_PREFIX + typedKeyValue.getName());
        String columnValue = (String) typedKeyValue.getValue();
        if (!stargateClaimValue.equals(columnValue)) {
          throw new UnauthorizedException("Not allowed to access this resource");
        }
      }
    }
  }
}
