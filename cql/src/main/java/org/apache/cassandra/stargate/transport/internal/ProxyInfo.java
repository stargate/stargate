package org.apache.cassandra.stargate.transport.internal;

import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;

public class ProxyInfo {
  static final AttributeKey<ProxyInfo> attributeKey = AttributeKey.valueOf("PROXY");

  public final InetSocketAddress publicAddress;
  public final InetSocketAddress sourceAddress;

  public ProxyInfo(InetSocketAddress publicAddress, InetSocketAddress sourceAddress) {
    this.publicAddress = publicAddress;
    this.sourceAddress = sourceAddress;
  }
}
