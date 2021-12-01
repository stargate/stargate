package io.grpc.netty.shaded.io.grpc.netty;

import static io.stargate.db.ClientInfo.PROXY_PUBLIC_ADDRESS_HEADER;
import static io.stargate.db.ClientInfo.PROXY_SOURCE_ADDRESS_HEADER;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.grpc.netty.shaded.io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.Map;

public class ProxyInfo {
  public static final AttributeKey<ProxyInfo> attributeKey = AttributeKey.valueOf("PROXY");

  public final InetSocketAddress destinationAddress;
  public final InetSocketAddress sourceAddress;
  private final Map<String, String> headers;

  public ProxyInfo(InetSocketAddress destinationAddress, InetSocketAddress sourceAddress) {
    this.destinationAddress = destinationAddress;
    this.sourceAddress = sourceAddress;
    this.headers =
        ImmutableMap.of(
            PROXY_SOURCE_ADDRESS_HEADER,
            sourceAddress.getAddress().getHostAddress(),
            PROXY_PUBLIC_ADDRESS_HEADER,
            destinationAddress.getAddress().getHostAddress());
  }

  public Map<String, String> toHeaders() {
    return headers;
  }

  @Override
  public String toString() {
    return "ProxyInfo{"
        + "destinationAddress="
        + destinationAddress
        + ", sourceAddress="
        + sourceAddress
        + ", headers="
        + headers
        + '}';
  }
}
