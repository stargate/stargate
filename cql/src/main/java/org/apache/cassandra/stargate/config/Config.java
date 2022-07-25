package org.apache.cassandra.stargate.config;

/**
 * This is the same as {@link org.apache.cassandra.config.Config}, but with only the subset of
 * options that are relevant for CQL transport. This object is used to deserialize configuration
 * from a yaml file using {@link YamlConfigurationLoader}. In general, these options should not be
 * accessed directly using this class, but code should instead use {@link
 * org.apache.cassandra.stargate.transport.internal.TransportDescriptor}.
 */
public class Config {

  public static final String PROPERTY_PREFIX = "stargate.cql.";

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
