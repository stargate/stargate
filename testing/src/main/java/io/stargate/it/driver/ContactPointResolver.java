package io.stargate.it.driver;

import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;

public interface ContactPointResolver {
  List<InetSocketAddress> resolve(ExtensionContext context) throws Exception;
}
