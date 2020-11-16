package io.stargate.it.proxy;

import com.google.common.net.InetAddresses;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateContainer;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * A test extension that manages instances of {@link TcpProxy}.
 *
 * <h3>Customization</h3>
 *
 * The behavior of this extension is modified using the {@link ProxySpec} annotation. If not present
 * it will use the defaults.
 *
 * <h3>Parameter injection</h3>
 *
 * This extension will inject a {@code List<InetSocketAddress>} parameter that's annotated with
 * {@link ProxyAddresses} which contains all the local addresses of the proxies managed by the
 * extension instance.
 */
public class ProxyExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {
  public static final ProxySpec DEFAULT_PROXY_SPEC = DefaultProxySpec.INSTANCE;

  private List<TcpProxy> proxies = new ArrayList<>();
  private List<InetSocketAddress> proxyAddresses = new ArrayList<>();
  private ProxySpec proxySpec;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    StargateEnvironmentInfo stargate =
        (StargateEnvironmentInfo)
            context.getStore(ExtensionContext.Namespace.GLOBAL).get(StargateContainer.STORE_KEY);
    if (stargate == null) {
      throw new IllegalStateException(
          String.format(
              "%s can only be used in conjunction with %s (make sure it is declared last)",
              ProxyExtension.class.getSimpleName(), StargateContainer.class.getSimpleName()));
    }

    proxySpec = getProxySpec(context);

    InetAddress localAddress = InetAddress.getByName(proxySpec.startingLocalAddress());
    final int numStargateNodes = stargate.nodes().size();
    for (int i = 0; i < proxySpec.numProxies(); ++i) {
      StargateConnectionInfo node = stargate.nodes().get(i % numStargateNodes);
      proxies.add(
          TcpProxy.builder()
              .localAddress(localAddress.getHostAddress(), proxySpec.localPort())
              .remoteAddress(node.seedAddress(), node.cqlPort())
              .build());
      proxyAddresses.add(new InetSocketAddress(localAddress, proxySpec.localPort()));
      localAddress = InetAddresses.increment(localAddress);
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    for (TcpProxy proxy : proxies) {
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
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    return type == List.class && (parameter.getAnnotation(ProxyAddresses.class) != null);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type == List.class && parameter.getAnnotation(ProxyAddresses.class) != null) {
      return proxyAddresses;
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
