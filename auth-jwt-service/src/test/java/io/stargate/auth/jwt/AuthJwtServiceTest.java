package io.stargate.auth.jwt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
}
