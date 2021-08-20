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
  private static Config conf = new Config();

  private static InetAddress rpcAddress;

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

  public static void daemonInitialization(Config config) {
    conf = config;
    applyAddressConfig();
  }

  public static void applyAddressConfig() {
    applyAddressConfig(conf);
  }

  public static void applyAddressConfig(Config config) {
    /* Local IP, hostname or interface to bind RPC server to */
    if (config.rpc_address != null && config.rpc_interface != null) {
      throw new ConfigurationException("Set rpc_address OR rpc_interface, not both", false);
    } else if (config.rpc_address != null) {
      try {
        rpcAddress = InetAddress.getByName(config.rpc_address);
      } catch (UnknownHostException e) {
        throw new ConfigurationException(
            "Unknown host in rpc_address " + config.rpc_address, false);
      }
    } else if (config.rpc_interface != null) {
      rpcAddress =
          getNetworkInterfaceAddress(
              config.rpc_interface, "rpc_interface", config.rpc_interface_prefer_ipv6);
    } else {
      rpcAddress = FBUtilities.getJustLocalAddress();
    }
  }

  public static int getNativeTransportPort() {
    return Integer.parseInt(
        System.getProperty(
            Config.PROPERTY_PREFIX + "native_transport_port",
            Integer.toString(conf.native_transport_port)));
  }

  public static int getNativeTransportPortSSL() {
    return conf.native_transport_port_ssl == null
        ? getNativeTransportPort()
        : conf.native_transport_port_ssl;
  }

  public static InetAddress getRpcAddress() {
    return rpcAddress;
  }

  public static boolean getRpcKeepAlive() {
    return conf.rpc_keepalive;
  }

  public static EncryptionOptions getNativeProtocolEncryptionOptions() {
    return conf.client_encryption_options;
  }

  public static long getNativeTransportMaxConcurrentRequestsInBytes() {
    return conf.native_transport_max_concurrent_requests_in_bytes;
  }

  public static void setNativeTransportMaxConcurrentRequestsInBytes(
      long maxConcurrentRequestsInBytes) {
    conf.native_transport_max_concurrent_requests_in_bytes = maxConcurrentRequestsInBytes;
  }

  public static long getNativeTransportMaxConcurrentRequestsInBytesPerIp() {
    return conf.native_transport_max_concurrent_requests_in_bytes_per_ip;
  }

  public static void setNativeTransportMaxConcurrentRequestsInBytesPerIp(
      long maxConcurrentRequestsInBytes) {
    conf.native_transport_max_concurrent_requests_in_bytes_per_ip = maxConcurrentRequestsInBytes;
  }

  public static long getNativeTransportMaxConcurrentConnections() {
    return conf.native_transport_max_concurrent_connections;
  }

  public static long getNativeTransportMaxConcurrentConnectionsPerIp() {
    return conf.native_transport_max_concurrent_connections_per_ip;
  }

  public static long nativeTransportIdleTimeout() {
    return conf.native_transport_idle_timeout_in_ms;
  }

  public static boolean useNativeTransportLegacyFlusher() {
    return conf.native_transport_flush_in_batches_legacy;
  }

  public static int getNativeTransportFrameBlockSize() {
    // TODO: Will need updated for protocol v5. The default of 32 was removed as part of this change
    // https://github.com/apache/cassandra/commit/a7c4ba9eeecb365e7c4753d8eaab747edd9a632a#diff-e966f41bc2a418becfe687134ec8cf542eb051eead7fb4917e65a3a2e7c9bce3L191
    return (int) ByteUnit.KIBI_BYTES.toBytes(32);
  }

  public static int getNativeTransportMaxFrameSize() {
    return (int) ByteUnit.MEBI_BYTES.toBytes(conf.native_transport_max_frame_size_in_mb);
  }

  public static boolean getNativeTransportAllowOlderProtocols() {
    return conf.native_transport_allow_older_protocols;
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
