package io.stargate.auth.jwt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTClaimsSet.Builder;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class AuthJwtServiceTest {

  private static final JwtValidator jwtValidator =
      mock(JwtValidator.class, withSettings().useConstructor("http://example.com"));

  private static final AuthJwtService authJwtService = new AuthJwtService(jwtValidator);

  @AfterEach
  void tearDown() {
    reset(jwtValidator);
  }

  @Test
  public void createTokenByKey() {
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> authJwtService.createToken("user"));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Test
  public void createToken() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class, () -> authJwtService.createToken("key", "secret"));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Test
  public void validateToken() throws UnauthorizedException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtValidator.validate("token")).thenReturn(jwtClaimsSet);

    StoredCredentials storedCredentials = authJwtService.validateToken("token");

    assertThat(storedCredentials).isNotNull();
    assertThat(storedCredentials.getRoleName()).isEqualTo("user");
  }

  @Test
  public void validateTokenMissingClaims() throws UnauthorizedException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("claims", stargate_claims).build();
    when(jwtValidator.validate("token")).thenReturn(jwtClaimsSet);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> authJwtService.validateToken("token"));
    assertThat(ex).hasMessage("java.text.ParseException: Missing field x-stargate-role for JWT");
  }

  @Test
  public void validateTokenMissingRole() throws UnauthorizedException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-foo", "user");
    JWTClaimsSet jwtClaimsSet = new Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtValidator.validate("token")).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> authJwtService.validateToken("token"));
    assertThat(ex).hasMessage("JWT must have a value for x-stargate-role");
  }

  @Test
  public void validateTokenInvalid() throws UnauthorizedException {
    when(jwtValidator.validate("token")).thenThrow(new UnauthorizedException("Invalid JWT"));

    UnauthorizedException ex =
        assertThrows(UnauthorizedException.class, () -> authJwtService.validateToken("token"));
    assertThat(ex).hasMessage("Invalid JWT");
  }

  @Test
  public void validateTokenMalformed() throws MalformedURLException {
    JwtValidator jwtValidator = new JwtValidator("http://example.com");
    AuthJwtService authJwtService = new AuthJwtService(jwtValidator);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> authJwtService.validateToken("token"));
    assertThat(ex)
        .hasMessage(
            "java.text.ParseException: Invalid JWT serialization: Missing dot delimiter(s)");
  }
}
