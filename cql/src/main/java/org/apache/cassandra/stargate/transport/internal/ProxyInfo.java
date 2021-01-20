package org.apache.cassandra.stargate.transport.internal;

import static io.stargate.db.ClientInfo.PROXY_PUBLIC_ADDRESS_HEADER;
import static io.stargate.db.ClientInfo.PROXY_SOURCE_ADDRESS_HEADER;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.Map;

public class ProxyInfo {
  public static final AttributeKey<ProxyInfo> attributeKey = AttributeKey.valueOf("PROXY");

  public final InetSocketAddress publicAddress;
  public final InetSocketAddress sourceAddress;
  private final Map<String, String> headers;

  public ProxyInfo(InetSocketAddress publicAddress, InetSocketAddress sourceAddress) {
    this.publicAddress = publicAddress;
    this.sourceAddress = sourceAddress;
    this.headers =
        ImmutableMap.of(
            PROXY_SOURCE_ADDRESS_HEADER,
            sourceAddress.getAddress().getHostAddress(),
            PROXY_PUBLIC_ADDRESS_HEADER,
            publicAddress.getAddress().getHostAddress());
  }

  public Map<String, String> toHeaders() {
    return headers;
  }
}
