package org.apache.cassandra.stargate.transport.internal;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public class TransportDescriptor {

  private enum ByteUnit {
    KIBI_BYTES(2048 * 1024, 1024),
    MEBI_BYTES(2048, 1024 * 1024);

    private final int overflowThreshold;
    private final int multiplier;

    ByteUnit(int t, int m) {
      this.overflowThreshold = t;
      this.multiplier = m;
    }

    public int overflowThreshold() {
      return overflowThreshold;
    }

    public boolean willOverflowInBytes(int val) {
      return val >= overflowThreshold;
    }

    public long toBytes(int val) {
      return (long) val * multiplier;
    }
  }

  private final Config conf;
  private final InetAddress rpcAddress;
  private final boolean internal;

  public TransportDescriptor(Config config) {
    this(config, computeRpcAddress(config), false);
  }

  private TransportDescriptor(Config conf, InetAddress rpcAddress, boolean internal) {
    this.conf = conf;
    this.rpcAddress = rpcAddress;
    this.internal = internal;
  }

  /** @see #isInternal() */
  public TransportDescriptor toInternal() {
    return new TransportDescriptor(conf, rpcAddress, true);
  }

  /**
   * Whether this is the "internal" transport used by Stargate services to query the persistence.
   */
  public boolean isInternal() {
    return internal;
  }

  public int getNativeTransportPort() {
    return Integer.parseInt(
        System.getProperty(
            Config.PROPERTY_PREFIX + "native_transport_port",
            Integer.toString(conf.native_transport_port)));
  }

  public int getNativeTransportPortSSL() {
    return conf.native_transport_port_ssl == null
        ? getNativeTransportPort()
        : conf.native_transport_port_ssl;
  }

  public InetAddress getRpcAddress() {
    return rpcAddress;
  }

  public boolean getRpcKeepAlive() {
    return conf.rpc_keepalive;
  }

  public EncryptionOptions getNativeProtocolEncryptionOptions() {
    return conf.client_encryption_options;
  }

  public long getNativeTransportMaxConcurrentRequestsInBytes() {
    return conf.native_transport_max_concurrent_requests_in_bytes;
  }

  public void setNativeTransportMaxConcurrentRequestsInBytes(long maxConcurrentRequestsInBytes) {
    conf.native_transport_max_concurrent_requests_in_bytes = maxConcurrentRequestsInBytes;
  }

  public long getNativeTransportMaxConcurrentRequestsInBytesPerIp() {
    return conf.native_transport_max_concurrent_requests_in_bytes_per_ip;
  }

  public void setNativeTransportMaxConcurrentRequestsInBytesPerIp(
      long maxConcurrentRequestsInBytes) {
    conf.native_transport_max_concurrent_requests_in_bytes_per_ip = maxConcurrentRequestsInBytes;
  }

  public long getNativeTransportMaxConcurrentConnections() {
    return conf.native_transport_max_concurrent_connections;
  }

  public long getNativeTransportMaxConcurrentConnectionsPerIp() {
    return conf.native_transport_max_concurrent_connections_per_ip;
  }

  public long nativeTransportIdleTimeout() {
    return conf.native_transport_idle_timeout_in_ms;
  }

  public boolean useNativeTransportLegacyFlusher() {
    return conf.native_transport_flush_in_batches_legacy;
  }

  public int getNativeTransportFrameBlockSize() {
    // TODO: Will need updated for protocol v5. The default of 32 was removed as part of this change
    // https://github.com/apache/cassandra/commit/a7c4ba9eeecb365e7c4753d8eaab747edd9a632a#diff-e966f41bc2a418becfe687134ec8cf542eb051eead7fb4917e65a3a2e7c9bce3L191
    return (int) ByteUnit.KIBI_BYTES.toBytes(32);
  }

  public int getNativeTransportMaxFrameSize() {
    return (int) ByteUnit.MEBI_BYTES.toBytes(conf.native_transport_max_frame_size_in_mb);
  }

  public boolean getNativeTransportAllowOlderProtocols() {
    return conf.native_transport_allow_older_protocols;
  }

  private static InetAddress computeRpcAddress(Config config) {
    /* Local IP, hostname or interface to bind RPC server to */
    if (config.rpc_address != null && config.rpc_interface != null) {
      throw new ConfigurationException("Set rpc_address OR rpc_interface, not both", false);
    } else if (config.rpc_address != null) {
      try {
        return InetAddress.getByName(config.rpc_address);
      } catch (UnknownHostException e) {
        throw new ConfigurationException(
            "Unknown host in rpc_address " + config.rpc_address, false);
      }
    } else if (config.rpc_interface != null) {
      return getNetworkInterfaceAddress(
          config.rpc_interface, "rpc_interface", config.rpc_interface_prefer_ipv6);
    } else {
      return FBUtilities.getJustLocalAddress();
    }
  }

  private static InetAddress getNetworkInterfaceAddress(
      String intf, String configName, boolean preferIPv6) throws ConfigurationException {
    try {
      NetworkInterface ni = NetworkInterface.getByName(intf);
      if (ni == null)
        throw new ConfigurationException(
            "Configured " + configName + " \"" + intf + "\" could not be found", false);
      Enumeration<InetAddress> addrs = ni.getInetAddresses();
      if (!addrs.hasMoreElements())
        throw new ConfigurationException(
            "Configured " + configName + " \"" + intf + "\" was found, but had no addresses",
            false);

      /*
       * Try to return the first address of the preferred type, otherwise return the first address
       */
      InetAddress retval = null;
      while (addrs.hasMoreElements()) {
        InetAddress temp = addrs.nextElement();
        if (preferIPv6 && temp instanceof Inet6Address) return temp;
        if (!preferIPv6 && temp instanceof Inet4Address) return temp;
        if (retval == null) retval = temp;
      }
      return retval;
    } catch (SocketException e) {
      throw new ConfigurationException(
          "Configured " + configName + " \"" + intf + "\" caused an exception", e);
    }
  }
}
