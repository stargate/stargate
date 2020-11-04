package io.stargate.auth.jwt;

import static io.stargate.auth.jwt.AuthnJwtService.CLAIMS_FIELD;
import static io.stargate.auth.jwt.AuthnJwtService.STARGATE_PREFIX;

import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import org.json.JSONException;
import org.json.JSONObject;

public class AuthzJwtService implements AuthorizationService {

  /**
   * Using the provided JWT and the claims it contains will perform pre-authorization where
   * possible, executes the query provided, and then authorizes the response of the query.
   *
   * @param action A {@link QueryBuilder} object to be executed and authorized against a JWT.
   * @param token The authenticated JWT token to use for authorization.
   * @param primaryKeyValues A list of primary key values that will be used in the query and should
   *     be authorized against the JWT.
   * @param tableMetadata The {@link Table} that will be queried against.
   * @return On success will return the result of the query and otherwise will return an exception
   *     relating to the failure to authorize.
   * @throws Exception An exception relating to the failure to authorize.
   */
  @Override
  public ResultSet authorizedDataRead(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckDataReadWrite(stargateClaims, primaryKeyValues, tableMetadata);

    ResultSet result = action.call();

    if (result == null) {
      return null;
    }

    postCheckDataRead(stargateClaims, result);

    return result;
  }

  /**
   * TODO
   *
   * @param action
   * @param token
   * @param primaryKeyValues
   * @param tableMetadata
   * @return
   * @throws Exception
   */
  @Override
  public ResultSet authorizedDataWrite(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckDataReadWrite(stargateClaims, primaryKeyValues, tableMetadata);

    // Just return the result. No value in doing a post check since we can't roll back anyway.
    return action.call();
  }

  @Override
  public ResultSet authorizedSchemaRead(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckSchemaRead(stargateClaims, keyspace, table);

    // TODO: [doug] 2020-11-2, Mon, 17:23 Finish implementing
    return action.call();
  }

  @Override
  public ResultSet authorizedSchemaWrite(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckSchemaWrite(stargateClaims, keyspace, table);

    // Just return the result. No value in doing a post check since we can't roll back anyway.
    return action.call();
  }

  private JSONObject extractClaimsFromJWT(String token) throws JSONException {
    // Grab the custom claims from the JWT. It's safe to work with the JWT as a plain Base64 encoded
    // json object here since by this point we've already authenticated the request.
    String[] parts = token.split("\\.");
    String decodedPayload = new String(Base64.getUrlDecoder().decode(parts[1]));
    JSONObject payload = new JSONObject(decodedPayload);
    return payload.getJSONObject(CLAIMS_FIELD);
  }

  private void preCheckSchemaRead(JSONObject stargateClaims, String keyspace, String table) {
    // TODO: [doug] 2020-11-2, Mon, 17:23 Finish implementing
  }

  private void preCheckSchemaWrite(JSONObject stargateClaims, String keyspace, String table) {
    // TODO: [doug] 2020-11-2, Mon, 17:23 Finish implementing
  }

  private void preCheckDataReadWrite(
      JSONObject stargateClaims, List<String> primaryKeyValues, Table tableMetadata)
      throws JSONException, UnauthorizedException {
    List<Column> keys = tableMetadata.primaryKeyColumns();

    if (primaryKeyValues.size() > keys.size()) {
      throw new IllegalArgumentException("Provided more primary key values than exists");
    }

    for (int i = 0; i < primaryKeyValues.size(); i++) {
      // If one of the columns exist as a field in the JWT claims and the values do not match then
      // the request is not allowed.
      if (stargateClaims.has(STARGATE_PREFIX + keys.get(i).name())) {

        if (!Column.ofTypeText(keys.get(i).type())) {
          throw new IllegalArgumentException(
              "Column must be of type text to be used for authorization");
        }

        String stargateClaimValue = stargateClaims.getString(STARGATE_PREFIX + keys.get(i).name());
        String columnValue = primaryKeyValues.get(i);
        if (!stargateClaimValue.equals(columnValue)) {
          throw new UnauthorizedException("Not allowed to access this resource");
        }
      }
    }
  }

  private void postCheckDataRead(JSONObject stargateClaims, ResultSet resultSet)
      throws JSONException, UnauthorizedException {
    if (resultSet == null || resultSet.rows() == null) {
      return;
    }

    // If scopes/fields line up with the resultset then return
    for (Row row : resultSet.rows()) {
      for (Column col : row.columns()) {
        if (stargateClaims.has(STARGATE_PREFIX + col.name())) {

          String stargateClaimValue = stargateClaims.getString(STARGATE_PREFIX + col.name());
          String columnValue = row.getString(col.name());
          if (!stargateClaimValue.equals(columnValue)) {
            throw new UnauthorizedException("Not allowed to access this resource");
          }
        }
      }
    }
  }
}
