package io.stargate.sgv2.api.common.logging;

import static io.stargate.sgv2.api.common.config.constants.LoggingConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.LoggingConfig;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

public class LoggingFilterTest {
  private static final String TEST_TENANT = "test-tenant";
  private static final String TEST_TENANT_2 = "test-tenant-2";

  @Test
  public void testIsLoggingNotAllowedWithDefaults()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, false, null, null, null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedWhenEnabled()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testIsLoggingNotAllowedForTenant()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig, true, Collections.singleton(TEST_TENANT_2), null, null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedForTenant()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig, true, Collections.singleton(TEST_TENANT), null, null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testIsLoggingNotAllowedForPath()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig, true, null, Collections.singleton("/api/rest/keyspaces2"), null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedForPath()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig, true, null, Collections.singleton("/api/rest/keyspaces"), null, null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testIsLoggingNotAllowedForPathPrefixes()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig, true, null, null, Collections.singleton("/api/rest/v1"), null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedForPathPrefixes()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, Set.of("/api/rest/", "/api/"), null, null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testIsLoggingNotAllowedForErrorCodes()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, null, Collections.singleton("400"), null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedForErrorCodes()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, null, Collections.singleton("400"), null);
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(400);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testIsLoggingNotAllowedForMethods()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, null, null, Collections.singleton("POST"));
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("GET");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert !isAllowed;
  }

  @Test
  public void testIsLoggingAllowedForMethods()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(loggingConfig, true, null, null, null, null, Collections.singleton("POST"));
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath()).thenReturn("/api/rest/keyspaces");
    when(requestContext.getMethod()).thenReturn("POST");
    when(responseContext.getStatus()).thenReturn(200);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;
  }

  @Test
  public void testLoggingWithNoRequestBody()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig,
        true,
        Collections.singleton(TEST_TENANT),
        Collections.singleton("/v2/keyspaces/clientkeyspace/clienttable"),
        Collections.singleton("/v2/keyspaces/"),
        Collections.singleton("400"),
        Collections.singleton("POST"));
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath())
        .thenReturn("/v2/keyspaces/clientkeyspace/clienttable");
    when(requestContext.getMethod()).thenReturn("POST");
    when(responseContext.getStatus()).thenReturn(400);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;

    Method buildRequestInfoMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "buildRequestInfo",
                ContainerRequestContext.class,
                ContainerResponseContext.class,
                boolean.class);
    buildRequestInfoMethod.setAccessible(true);
    String requestInfo =
        (String)
            buildRequestInfoMethod.invoke(loggingFilter, requestContext, responseContext, true);
    // assert equals
    String expectedRequestInfo =
        String.format(
            "REQUEST INFO :: %s %s %s %s %s",
            TEST_TENANT, 400, "POST", "/v2/keyspaces/clientkeyspace/clienttable", "");
    assertThat(requestInfo).isEqualTo(expectedRequestInfo);
  }

  @Test
  public void testLoggingWithRequestBody()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    ContainerResponseContext responseContext = mock(ContainerResponseContext.class);
    LoggingConfig loggingConfig = mock(LoggingConfig.class);
    StargateRequestInfo stargateRequestInfo = mock(StargateRequestInfo.class);
    mockLoggingConfig(
        loggingConfig,
        true,
        Collections.singleton(TEST_TENANT),
        Collections.singleton("/v2/keyspaces/clientkeyspace/clienttable"),
        Collections.singleton("/v2/keyspaces/"),
        Collections.singleton("400"),
        Collections.singleton("POST"));
    mockStargateRequestInfo(stargateRequestInfo);
    when(requestContext.getUriInfo()).thenReturn(mock(jakarta.ws.rs.core.UriInfo.class));
    when(requestContext.getUriInfo().getPath())
        .thenReturn("/v2/keyspaces/clientkeyspace/clienttable");
    when(requestContext.getMethod()).thenReturn("POST");
    String requestBody = "{\"name\":\"test\"}";
    when(requestContext.getEntityStream())
        .thenReturn(new ByteArrayInputStream(Strings.toByteArray(requestBody)));
    when(responseContext.getStatus()).thenReturn(400);
    when(responseContext.getStatus()).thenReturn(400);

    LoggingFilter loggingFilter =
        new LoggingFilter(new ObjectMapper(), stargateRequestInfo, loggingConfig);
    Method isAllowedMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "isLoggingAllowed", ContainerRequestContext.class, ContainerResponseContext.class);
    isAllowedMethod.setAccessible(true);
    boolean isAllowed =
        (boolean) isAllowedMethod.invoke(loggingFilter, requestContext, responseContext);
    assert isAllowed;

    Method buildRequestInfoMethod =
        loggingFilter
            .getClass()
            .getDeclaredMethod(
                "buildRequestInfo",
                ContainerRequestContext.class,
                ContainerResponseContext.class,
                boolean.class);
    buildRequestInfoMethod.setAccessible(true);
    String requestInfo =
        (String)
            buildRequestInfoMethod.invoke(loggingFilter, requestContext, responseContext, true);
    // assert equals
    String expectedRequestInfo =
        String.format(
            "REQUEST INFO :: %s %s %s %s %s",
            TEST_TENANT, 400, "POST", "/v2/keyspaces/clientkeyspace/clienttable", requestBody);
    assertThat(requestInfo).isEqualTo(expectedRequestInfo);
  }

  private void mockStargateRequestInfo(StargateRequestInfo stargateRequestInfo) {
    when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of(TEST_TENANT));
  }

  private void mockLoggingConfig(
      LoggingConfig loggingConfig,
      boolean loggingEnabled,
      Set<String> tenants,
      Set<String> paths,
      Set<String> pathPrefixes,
      Set<String> errorCodes,
      Set<String> methods) {
    when(loggingConfig.enabled()).thenReturn(loggingEnabled);
    when(loggingConfig.enabledTenants())
        .thenReturn(
            tenants != null
                ? Optional.of(tenants)
                : Optional.of(Collections.singleton(ALL_TENANTS)));
    when(loggingConfig.enabledPaths())
        .thenReturn(
            paths != null ? Optional.of(paths) : Optional.of(Collections.singleton(ALL_PATHS)));
    when(loggingConfig.enabledPathPrefixes())
        .thenReturn(
            pathPrefixes != null
                ? Optional.of(pathPrefixes)
                : Optional.of(Collections.singleton(ALL_PATH_PREFIXES)));
    when(loggingConfig.enabledErrorCodes())
        .thenReturn(
            errorCodes != null
                ? Optional.of(errorCodes)
                : Optional.of(Collections.singleton(ALL_ERROR_CODES)));
    when(loggingConfig.enabledMethods())
        .thenReturn(
            methods != null
                ? Optional.of(methods)
                : Optional.of(Collections.singleton(ALL_METHODS)));
  }
}
