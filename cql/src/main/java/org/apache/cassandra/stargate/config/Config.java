package org.apache.cassandra.stargate.config;

public class Config {

  public static final String PROPERTY_PREFIX = "stargate.cql.";

  /*
   * RPC address and interface refer to the address/interface used for the native protocol used to communicate with
   * clients. It's still called RPC in some places even though Thrift RPC is gone. If you see references to native
   * address or native port it's derived from the RPC address configuration.
   *
   * native_transport_port is the port that is paired with RPC address to bind on.
   */
  public String rpc_address;
  public String rpc_interface;
  public boolean rpc_interface_prefer_ipv6 = false;
  public boolean rpc_keepalive = true;

  public int native_transport_port = 9042;
  public Integer native_transport_port_ssl = null;
  public int native_transport_max_frame_size_in_mb = 256;
  public volatile long native_transport_max_concurrent_connections = -1L;
  public volatile long native_transport_max_concurrent_connections_per_ip = -1L;
  public boolean native_transport_flush_in_batches_legacy = false;
  public volatile boolean native_transport_allow_older_protocols = true;
  public volatile long native_transport_max_concurrent_requests_in_bytes_per_ip = -1L;
  public volatile long native_transport_max_concurrent_requests_in_bytes = -1L;

  public long native_transport_idle_timeout_in_ms = 0L;

  public EncryptionOptions client_encryption_options = new EncryptionOptions();
}
