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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import java.util.Hashtable;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;

public class AuthJWTServiceActivatorTest {

  @Test
  public void shouldRegisterIfSelectedAuthProvider() {
    System.setProperty("stargate.auth.jwt_provider_url", "http://example.com");
    System.setProperty("stargate.auth_id", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    activator.start(bundleContext);

    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, times(1))
        .registerService(
            eq(AuthenticationService.class.getName()),
            any(AuthnJwtService.class),
            eq(expectedProps));
    verify(bundleContext, times(1))
        .registerService(
            eq(AuthorizationService.class.getName()),
            any(AuthzJwtService.class),
            eq(expectedProps));
  }

  @Test
  public void shouldNotRegisterIfNotSelectedAuthProvider() {
    System.setProperty("stargate.auth.jwt_provider_url", "http://example.com");
    System.setProperty("stargate.auth_id", "foo");
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    activator.start(bundleContext);

    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthenticationService.class.getName()),
            any(AuthnJwtService.class),
            eq(expectedProps));
    verify(bundleContext, never())
        .registerService(
            eq(AuthorizationService.class.getName()),
            any(AuthzJwtService.class),
            eq(expectedProps));
  }

  @Test
  public void shouldNotRegisterIfMissingURL() {
    System.clearProperty("stargate.auth.jwt_provider_url");
    System.setProperty("stargate.auth_id", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> activator.start(bundleContext));
    assertThat(ex).hasMessage("Property `stargate.auth.jwt_provider_url` must be set");

    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthenticationService.class.getName()),
            any(AuthnJwtService.class),
            eq(expectedProps));
    verify(bundleContext, never())
        .registerService(
            eq(AuthorizationService.class.getName()),
            any(AuthzJwtService.class),
            eq(expectedProps));
  }

  @Test
  public void shouldNotRegisterIfEmptyURL() {
    System.setProperty("stargate.auth.jwt_provider_url", "");
    System.setProperty("stargate.auth_id", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> activator.start(bundleContext));
    assertThat(ex).hasMessage("Property `stargate.auth.jwt_provider_url` must be set");

    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthenticationService.class.getName()),
            any(AuthnJwtService.class),
            eq(expectedProps));
    verify(bundleContext, never())
        .registerService(
            eq(AuthorizationService.class.getName()),
            any(AuthzJwtService.class),
            eq(expectedProps));
  }

  @Test
  public void shouldNotRegisterIfInvalidURL() {
    System.setProperty("stargate.auth.jwt_provider_url", "foo");
    System.setProperty("stargate.auth_id", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> activator.start(bundleContext));
    assertThat(ex).hasMessage("Failed to create JwtValidator: no protocol: foo");

    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthenticationService.class.getName()),
            any(AuthnJwtService.class),
            eq(expectedProps));
    verify(bundleContext, never())
        .registerService(
            eq(AuthorizationService.class.getName()),
            any(AuthzJwtService.class),
            eq(expectedProps));
  }
}
