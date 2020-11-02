package io.stargate.auth.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import io.stargate.auth.AuthnzService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.text.ParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthJwtService implements AuthnzService {

  private static final Logger logger = LoggerFactory.getLogger(AuthJwtService.class);

  private static final String STARGATE_PREFIX = "x-stargate-";
  private static final String ROLE_FIELD = STARGATE_PREFIX + "role";
  private static final String CLAIMS_FIELD = "stargate_claims";

  private final ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor;

  public AuthJwtService(ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor) {
    this.jwtProcessor = jwtProcessor;
  }

  @Override
  public String createToken(String key, String secret) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Override
  public String createToken(String key) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  /**
   * Validates a token in the form of a JWT to ensure that 1) it's not expired, 2) it's correctly
   * signed by the provider, and 3) contains the proper role for the given DB.
   *
   * @param token A JWT created by an auth provider.
   * @return A {@link StoredCredentials} containing the role name the request is authenticated to
   *     use.
   * @throws UnauthorizedException An UnauthorizedException if the JWT is expired, malformed, or not
   *     properly signed.
   */
  @Override
  public StoredCredentials validateToken(String token) throws UnauthorizedException {
    JWTClaimsSet claimsSet = validate(token);
    String roleName;
    try {
      roleName = getRoleForJWT(claimsSet.getJSONObjectClaim(CLAIMS_FIELD));
    } catch (IllegalArgumentException | ParseException e) {
      logger.info("Failed to parse claim from JWT", e);
      throw new UnauthorizedException("Failed to parse claim from JWT", e);
    }

    if (roleName == null || roleName.equals("")) {
      throw new UnauthorizedException("JWT must have a value for " + ROLE_FIELD);
    }

    StoredCredentials storedCredentials = new StoredCredentials();
    storedCredentials.setRoleName(roleName);
    return storedCredentials;
  }

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
  public ResultSet executeDataReadWithAuthorization(
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
  public ResultSet executeDataWriteWithAuthorization(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckDataReadWrite(stargateClaims, primaryKeyValues, tableMetadata);

    // Just return the result. No value in doing a post check since we can't roll back anyway.
    return action.call();
  }

  @Override
  public ResultSet executeSchemaReadWithAuthorization(
      Callable<ResultSet> action, String token, String keyspace, String table) throws Exception {
    JSONObject stargateClaims = extractClaimsFromJWT(token);

    preCheckSchemaRead(stargateClaims, keyspace, table);

    // TODO: [doug] 2020-11-2, Mon, 17:23 Finish implementing
    return action.call();
  }

  @Override
  public ResultSet executeSchemaWriteWithAuthorization(
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

  /**
   * For a given JWT check that it is valid which means
   *
   * <p>
   *
   * <ol>
   *   <li>Hasn't expired
   *   <li>Properly signed
   *   <li>Isn't malformed
   * </ol>
   *
   * <p>
   *
   * @param token The JWT to be validated
   * @return Will return the {@link JWTClaimsSet} if the token is valid, otherwise an exception will
   *     be thrown.
   * @throws UnauthorizedException The exception returned for JWTs that are known invalid such as
   *     expired or not signed. If an error occurs while parsing a RuntimeException is thrown.
   */
  private JWTClaimsSet validate(String token) throws UnauthorizedException {
    JWTClaimsSet claimsSet;
    try {
      claimsSet = jwtProcessor.process(token, null); // context is an optional param so passing null
    } catch (ParseException | JOSEException e) {
      logger.info("Failed to process JWT", e);
      throw new UnauthorizedException("Failed to process JWT: " + e.getMessage(), e);
    } catch (BadJOSEException badJOSEException) {
      logger.info("Tried to validate invalid JWT", badJOSEException);
      throw new UnauthorizedException(
          "Invalid JWT: " + badJOSEException.getMessage(), badJOSEException);
    }

    return claimsSet;
  }

  private String getRoleForJWT(Map<String, Object> stargate_claims) {
    if (stargate_claims == null) {
      throw new IllegalArgumentException("Missing field " + ROLE_FIELD + " for JWT");
    }

    if (!(stargate_claims.get(ROLE_FIELD) instanceof String)) {
      throw new IllegalArgumentException("Field " + ROLE_FIELD + " must be of type String");
    }

    return (String) stargate_claims.get(ROLE_FIELD);
  }
}
