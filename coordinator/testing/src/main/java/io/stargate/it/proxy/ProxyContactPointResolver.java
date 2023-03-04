package io.stargate.it.proxy;

import io.stargate.it.driver.ContactPointResolver;
import io.stargate.it.proxy.ProxyExtension.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ProxyContactPointResolver implements ContactPointResolver {
  @Override
  public List<InetSocketAddress> resolve(ExtensionContext context) throws Exception {
    Proxy proxy =
        (Proxy) context.getStore(ExtensionContext.Namespace.GLOBAL).get(ProxyExtension.STORE_KEY);
    if (proxy == null) {
      throw new IllegalStateException(
          String.format(
              "%s requires the use of %s (make sure it is declared first)",
              ProxyContactPointResolver.class.getSimpleName(),
              ProxyExtension.class.getSimpleName()));
    }
    return proxy.addresses();
  }
}
