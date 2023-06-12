/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.config;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

/**
 * This is the same as {@link org.apache.cassandra.config.Config}, but with only the subset of
 * options that are relevant for CQL transport. In general, these options should not be accessed
 * directly using this class, but code should instead use {@link
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

  public int native_transport_receive_queue_capacity_in_bytes = 1048576;
  public volatile int consecutive_message_errors_threshold = 20;

  public long native_transport_idle_timeout_in_ms = 0L;

  @JsonSetter(nulls = Nulls.FAIL)
  public EncryptionOptions client_encryption_options = new EncryptionOptions();

  public Integer networking_cache_size_in_mb;

  public Integer file_cache_size_in_mb;
}
