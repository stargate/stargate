package io.stargate.it.driver;

import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateExtension;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DefaultContactPointResolver implements ContactPointResolver {
  @Override
  public List<InetSocketAddress> resolve(ExtensionContext context) {
    StargateEnvironmentInfo stargate =
        (StargateEnvironmentInfo)
            context.getStore(ExtensionContext.Namespace.GLOBAL).get(StargateExtension.STORE_KEY);
    if (stargate == null) {
      throw new IllegalStateException(
          String.format(
              "%s can only be used in conjunction with %s (make sure it is declared last)",
              CqlSessionExtension.class.getSimpleName(), StargateExtension.class.getSimpleName()));
    }
    List<InetSocketAddress> contactPoints = new ArrayList<>();
    for (StargateConnectionInfo node : stargate.nodes()) {
      contactPoints.add(new InetSocketAddress(node.seedAddress(), node.cqlPort()));
    }
    return contactPoints;
  }
}
