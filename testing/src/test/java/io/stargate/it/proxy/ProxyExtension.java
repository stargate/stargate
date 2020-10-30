package io.stargate.it.proxy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.netty.channel.local.LocalAddress;
import io.stargate.it.driver.CqlSessionHelper;
import io.stargate.it.driver.TestKeyspace;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;

public class ProxyExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {
  private TcpProxy proxy;
  private ProxySpec proxySpec;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    proxySpec = getProxySpec(extensionContext);
    proxy =
        TcpProxy.builder()
            .localAddress(proxySpec.localAddress(), proxySpec.localPort())
            .remoteAddress(proxySpec.remoteAddress(), proxySpec.remotePort())
            .build();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (proxy != null) {
      proxy.close();
    }
  }

  private ProxySpec getProxySpec(ExtensionContext context) {
    AnnotatedElement element =
        context
            .getElement()
            .orElseThrow(() -> new IllegalStateException("Expected to have an element"));
    Optional<ProxySpec> maybeSpec = AnnotationSupport.findAnnotation(element, ProxySpec.class);
    if (!maybeSpec.isPresent() && element instanceof Method) {
      maybeSpec =
          AnnotationSupport.findAnnotation(((Method) element).getDeclaringClass(), ProxySpec.class);
    }
    return maybeSpec.orElse(DefaultProxySpec.INSTANCE);
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext,
      ExtensionContext extensionContext) throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    return type == InetSocketAddress.class && (parameter.getAnnotation(ProxyAddress.class) != null );
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext,
      ExtensionContext extensionContext) throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type == InetSocketAddress.class && parameter.getAnnotation(ProxyAddress.class) != null) {
      return new InetSocketAddress(proxySpec.localAddress(), proxySpec.localPort());
    } else {
      throw new AssertionError("Unsupported parameter");
    }
  }

  @ProxySpec
  private static class DefaultProxySpec {
    private static final ProxySpec INSTANCE =
        ProxyExtension.DefaultProxySpec.class.getAnnotation(ProxySpec.class);
  }
}
