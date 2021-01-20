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
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.stargate.auth.AuthenticationSubject;
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

public class AuthnJwtServiceTest {

  public static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();
  private DefaultJWTProcessor<?> jwtProcessorMocked;
  private AuthnJwtService mockAuthnJwtService;
  private AuthnJwtService liveAuthnJwtService;
  private SecretKey key;

  @BeforeEach
  void setup() {
    jwtProcessorMocked = mock(DefaultJWTProcessor.class);
    mockAuthnJwtService = new AuthnJwtService(jwtProcessorMocked);

    byte[] keyBytes = new byte[32];
    new SecureRandom().nextBytes(keyBytes);
    key = new SecretKeySpec(keyBytes, JWSAlgorithm.RS256.getName());

    ConfigurableJWTProcessor<SimpleSecurityContext> processor = new DefaultJWTProcessor<>();
    processor.setJWSKeySelector((header, context) -> Collections.singletonList(key));
    liveAuthnJwtService = new AuthnJwtService(processor);
  }

  @Test
  public void createTokenByKey() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> mockAuthnJwtService.createToken("user", EMPTY_HEADERS));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthnJwtService. Tokens must be created out of band.");
  }

  @Test
  public void createToken() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> mockAuthnJwtService.createToken("key", "secret", EMPTY_HEADERS));
    assertThat(ex)
        .hasMessage(
            "Creating a token is not supported for AuthnJwtService. Tokens must be created out of band.");
  }

  @Test
  public void validateToken()
      throws UnauthorizedException, ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    AuthenticationSubject authenticationSubject =
        mockAuthnJwtService.validateToken("token", EMPTY_HEADERS);

    assertThat(authenticationSubject).isNotNull();
    assertThat(authenticationSubject.roleName()).isEqualTo("user");
  }

  @Test
  public void validateTokenMissingClaims() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "user");
    JWTClaimsSet jwtClaimsSet = new JWTClaimsSet.Builder().claim("claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> mockAuthnJwtService.validateToken("token", EMPTY_HEADERS));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenMissingRole() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-foo", "user");
    JWTClaimsSet jwtClaimsSet =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> mockAuthnJwtService.validateToken("token", EMPTY_HEADERS));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenEmptyRole() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "");
    JWTClaimsSet jwtClaimsSet =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> mockAuthnJwtService.validateToken("token", EMPTY_HEADERS));
    assertThat(ex).hasMessage("JWT must have a value for x-stargate-role");
  }

  @Test
  public void validateTokenRoleWrongType() throws ParseException, JOSEException, BadJOSEException {
    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", 1);
    JWTClaimsSet jwtClaimsSet =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();
    when(jwtProcessorMocked.process("token", null)).thenReturn(jwtClaimsSet);

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> mockAuthnJwtService.validateToken("token", EMPTY_HEADERS));
    assertThat(ex).hasMessage("Failed to parse claim from JWT");
  }

  @Test
  public void validateTokenInvalid() throws ParseException, JOSEException, BadJOSEException {
    when(jwtProcessorMocked.process("token", null))
        .thenThrow(new BadJOSEException("The provided JWT is bad"));

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> mockAuthnJwtService.validateToken("token", EMPTY_HEADERS));
    assertThat(ex).hasMessage("Invalid JWT: The provided JWT is bad");
  }

  @Test
  public void validateTokenMalformed() {
    ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
    AuthnJwtService authnJwtService = new AuthnJwtService(jwtProcessor);

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () -> authnJwtService.validateToken("token", EMPTY_HEADERS));
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
            UnauthorizedException.class,
            () -> liveAuthnJwtService.validateToken(jwt.serialize(), EMPTY_HEADERS));
    assertThat(ex).hasMessage("Invalid JWT: Expired JWT");
  }
}
