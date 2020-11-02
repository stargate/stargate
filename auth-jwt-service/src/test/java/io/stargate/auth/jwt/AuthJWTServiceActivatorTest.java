package io.stargate.auth.jwt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.stargate.auth.AuthnzService;
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

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, times(1))
        .registerService(
            eq(AuthnzService.class.getName()), any(AuthJwtService.class), eq(expectedProps));
  }

  @Test
  public void shouldNotRegisterIfNotSelectedAuthProvider() {
    System.setProperty("stargate.auth.jwt_provider_url", "http://example.com");
    System.setProperty("stargate.auth_id", "foo");
    BundleContext bundleContext = mock(BundleContext.class);
    AuthJWTServiceActivator activator = new AuthJWTServiceActivator();

    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthnzService.class.getName()), any(AuthJwtService.class), eq(expectedProps));
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

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthnzService.class.getName()), any(AuthJwtService.class), eq(expectedProps));
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

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthnzService.class.getName()), any(AuthJwtService.class), eq(expectedProps));
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

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("AuthIdentifier", AuthJWTServiceActivator.AUTH_JWT_IDENTIFIER);

    verify(bundleContext, never())
        .registerService(
            eq(AuthnzService.class.getName()), any(AuthJwtService.class), eq(expectedProps));
  }
}
