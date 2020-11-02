package io.stargate.auth.jwt;

import static io.stargate.auth.jwt.SampleTable.SHOPPING_CART;
import static io.stargate.auth.jwt.SampleTable.SHOPPING_CART_NON_TEXT_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.proc.SimpleSecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTClaimsSet.Builder;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthJwtServiceTest {

  private DefaultJWTProcessor<?> jwtProcessorMocked;
  private AuthJwtService mockAuthJwtService;
  private AuthJwtService liveAuthJwtService;
  private SecretKey key;

  @BeforeEach
  void setup() {
    jwtProcessorMocked = mock(DefaultJWTProcessor.class);
    mockAuthJwtService = new AuthJwtService(jwtProcessorMocked);

    byte[] keyBytes = new byte[32];
    new SecureRandom().nextBytes(keyBytes);
    key = new SecretKeySpec(keyBytes, JWSAlgorithm.RS256.getName());

    ConfigurableJWTProcessor<SimpleSecurityContext> processor = new DefaultJWTProcessor<>();
    processor.setJWSKeySelector((header, context) -> Collections.singletonList(key));
    liveAuthJwtService = new AuthJwtService(processor);
  }

  @Test
  public void createTokenByKey() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class, () -> mockAuthJwtService.createToken("user"));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Test
  public void createToken() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> mockAuthJwtService.createToken("key", "secret"));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Test
  public void validateToken()
      throws UnauthorizedException, ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    StoredCredentials storedCredentials = mockAuthJwtService.validateToken("token");

    assertThat(storedCredentials).isNotNull();
    assertThat(storedCredentials.getRoleName()).isEqualTo("user");
  }

  @Test
  public void validateTokenMissingClaims() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> mockAuthJwtService.validateToken("token"));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenMissingRole() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-foo", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> mockAuthJwtService.validateToken("token"));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenEmptyRole() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> mockAuthJwtService.validateToken("token"));
    assertThat(ex).hasMessage("JWT must have a value for x-stargate-role");
  }

  @Test
  public void validateTokenRoleWrongType() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", 1);
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> mockAuthJwtService.validateToken("token"));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenInvalid() throws ParseException, JOSEException, BadJOSEException {
    when(jwtProcessorMocked.process("token", null))
        .thenThrow(new BadJOSEException("The provided JWT is bad"));

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> mockAuthJwtService.validateToken("token"));
    assertThat(ex).hasMessage("Invalid JWT: The provided JWT is bad");
  }

  @Test
  public void validateTokenMalformed() {
    ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
    AuthJwtService authJwtService = new AuthJwtService(jwtProcessor);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> authJwtService.validateToken("token"));
    assertThat(ex)
        .hasMessage("Failed to process JWT: Invalid JWT serialization: Missing dot delimiter(s)");
  }

  @Test
  public void validateTokenExpired() throws JOSEException {
    final Date now = new Date();
    final Date yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    JWTClaimsSet claims =
        new JWTClaimsSet.Builder().subject("alice").expirationTime(yesterday).build();

    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    jwt.sign(new MACSigner(key));

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class, () -> liveAuthJwtService.validateToken(jwt.serialize()));
    assertThat(ex).hasMessage("Invalid JWT: Expired JWT");
  }

  @Test
  public void executeDataReadWithAuthorization() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<String> primaryKeyValues = Collections.singletonList("123");

    ResultSet result =
        mockAuthJwtService.executeDataReadWithAuthorization(
            action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART);
    assertThat(result.rows().get(0)).isEqualTo(row);
  }

  @Test
  public void executeDataReadWithAuthorizationNullResultSet() throws Exception {
    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(null);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<String> primaryKeyValues = Collections.singletonList("123");

    ResultSet result =
        mockAuthJwtService.executeDataReadWithAuthorization(
            action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART);
    assertThat(result).isNull();
  }

  @Test
  public void executeDataReadWithAuthorizationResultSetWithNoRows() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.rows()).thenReturn(null);

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<String> primaryKeyValues = Collections.singletonList("123");

    ResultSet result =
        mockAuthJwtService.executeDataReadWithAuthorization(
            action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART);
    assertThat(result.rows()).isEqualTo(null);
  }

  @Test
  public void executeDataReadWithAuthorizationMoreValuesThanKeys() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<String> primaryKeyValues = Arrays.asList("123", "abc");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mockAuthJwtService.executeDataReadWithAuthorization(
                    action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART));
    assertThat(ex).hasMessage("Provided more primary key values than exists");
  }

  @Test
  public void executeDataReadWithAuthorizationNotAuthorized() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "456");

    List<String> primaryKeyValues = Collections.singletonList("123");

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () ->
                mockAuthJwtService.executeDataReadWithAuthorization(
                    action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART));
    assertThat(ex).hasMessage("Not allowed to access this resource");
  }

  @Test
  public void executeDataReadWithAuthorizationNotAuthorizedResult() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "456");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<String> primaryKeyValues = Collections.singletonList("123");

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () ->
                mockAuthJwtService.executeDataReadWithAuthorization(
                    action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART));
    assertThat(ex).hasMessage("Not allowed to access this resource");
  }

  @Test
  public void executeDataReadWithAuthorizationClaimOnNonTextColumn() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART_NON_TEXT_PK.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");
    stargate_claims.put("x-stargate-item_count", 2);

    List<String> primaryKeyValues = Arrays.asList("123", "2");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mockAuthJwtService.executeDataReadWithAuthorization(
                    action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART_NON_TEXT_PK));
    assertThat(ex).hasMessage("Column must be of type text to be used for authorization");
  }

  @Test
  public void executeDataReadWithAuthorizationNoClaim() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values1 = new HashMap<>();
    values1.put("userid", "123");
    values1.put("item_count", 2);
    values1.put("last_update_timestamp", Instant.now());
    Row row1 = createRow(SHOPPING_CART.columns(), values1);

    Map<String, Object> values2 = new HashMap<>();
    values2.put("userid", "456");
    values2.put("item_count", 4);
    values2.put("last_update_timestamp", Instant.now());
    Row row2 = createRow(SHOPPING_CART.columns(), values2);

    when(resultSet.rows()).thenReturn(Arrays.asList(row1, row2));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "admin");

    List<String> primaryKeyValues = Collections.emptyList();

    ResultSet result =
        mockAuthJwtService.executeDataReadWithAuthorization(
            action, signJWT(stargate_claims), primaryKeyValues, SHOPPING_CART);
    assertThat(result.rows().get(0)).isEqualTo(row1);
    assertThat(result.rows().get(1)).isEqualTo(row2);
  }

  private String signJWT(Map<String, Object> stargate_claims) throws JOSEException {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();

    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    jwt.sign(new MACSigner(key));

    return jwt.serialize();
  }

  private Row createRow(List<Column> columns, Map<String, Object> data) {
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    for (Column column : columns) {
      Object v = data.get(column.name());
      values.add(v == null ? null : column.type().codec().encode(v, ProtocolVersion.DEFAULT));
    }
    return new ArrayListBackedRow(columns, values, ProtocolVersion.DEFAULT);
  }
}
