package io.stargate.it.proxy;

import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import io.stargate.it.storage.ExternalResource;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateExtension;
import java.lang.reflect.Parameter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ProxyExtension extends ExternalResource<ProxySpec, ProxyExtension.Proxy>
    implements ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyExtension.class);

  public static final String STORE_KEY = "stargate-proxy";

  public static final ProxySpec DEFAULT_PROXY_SPEC = DefaultProxySpec.INSTANCE;

  public ProxyExtension() {
    super(ProxySpec.class, STORE_KEY, Namespace.GLOBAL);
  }

  @Override
  protected boolean isShared(ProxySpec spec) {
    return spec.shared();
  }

  @Override
  protected Optional<Proxy> processResource(
      Proxy existingResource, ProxySpec spec, ExtensionContext context) throws Exception {
    StargateEnvironmentInfo stargate =
        (StargateEnvironmentInfo)
            context.getStore(ExtensionContext.Namespace.GLOBAL).get(StargateExtension.STORE_KEY);
    if (stargate == null) {
      throw new IllegalStateException(
          String.format(
              "%s can only be used in conjunction with %s (make sure it is declared last)",
              ProxyExtension.class.getSimpleName(), StargateExtension.class.getSimpleName()));
    }
    return Optional.of(new Proxy(spec != null ? spec : DEFAULT_PROXY_SPEC, stargate));
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    return type == List.class && (parameter.getAnnotation(ProxyAddresses.class) != null);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type == List.class && parameter.getAnnotation(ProxyAddresses.class) != null) {
      return proxy(context).addresses();
    } else {
      throw new AssertionError("Unsupported parameter");
    }
  }

  @ProxySpec
  private static class DefaultProxySpec {
    private static final ProxySpec INSTANCE =
        ProxyExtension.DefaultProxySpec.class.getAnnotation(ProxySpec.class);
  }

  private Proxy proxy(ExtensionContext context) {
    return getResource(context)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Proxy has not been configured in " + context.getUniqueId()));
  }

  protected static class Proxy extends ExternalResource.Holder {
    private final List<TcpProxy> proxies = new ArrayList<>();
    private final List<InetSocketAddress> addresses = new ArrayList<>();

    List<InetSocketAddress> addresses() {
      return addresses;
    }

    public Proxy(ProxySpec spec, StargateEnvironmentInfo stargate) throws Exception {
      InetAddress localAddress = InetAddress.getByName(spec.startingLocalAddress());
      final int numStargateNodes = stargate.nodes().size();
      for (int i = 0; i < spec.numProxies(); ++i) {
        StargateConnectionInfo node = stargate.nodes().get(i % numStargateNodes);
        proxies.add(
            TcpProxy.builder()
                .localAddress(localAddress.getHostAddress(), spec.localPort())
                .remoteAddress(node.seedAddress(), node.cqlPort())
                .build());
        addresses.add(new InetSocketAddress(localAddress, spec.localPort()));
        localAddress = InetAddresses.increment(localAddress);
      }
    }

    @Override
    public void close() {
      for (TcpProxy proxy : proxies) {
        try {
          proxy.close();
        } catch (InterruptedException e) {
          LOG.error("Unable to close proxy", e);
        }
      }
      super.close();
    }
  }
}
