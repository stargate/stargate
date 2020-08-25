package org.apache.cassandra.stargate.transport.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.netty.util.AttributeKey;

public class ProxyInfo
{
    static final AttributeKey<ProxyInfo> attributeKey = AttributeKey.valueOf("PROXY");

    public final InetSocketAddress publicAddress;

    public ProxyInfo(InetSocketAddress publicAddress)
    {
        this.publicAddress = publicAddress;
    }

}
