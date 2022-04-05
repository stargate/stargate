/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.search.solr.dht;

import org.apache.cassandra.config.PropertyConfiguration;

public class ShardRouterConstants {
  public static final String EXCLUDED_HOSTS_PROPERTY = "dse.routing.exclude.hosts";
  public static final String PREFER_REMOTE_PROPERTY = "dse.routing.prefer.remote";
  public static final String QUERY_BY_RANGE = "query.range";
  public static final String QUERY_BY_PARTITION = "query.partition";
  public static final String ROUTE_BY_PARTITION = "route.partition";
  public static final String ROUTE_BY_RANGE = "route.range";
  public static final String SHARD_RANGES_PARAM_PREFIX = "shard.ranges/";
  public static final String SHARDS_INFO_TOKEN_RANGES = "tokenRanges";
  // public static final String SHARD_COORDINATOR_ID = ShardRouter.class.getSimpleName() +
  // ".SHARD_COORDINATOR_ID";
  // public static final String SHARD_COORDINATOR_IP = ShardRouter.class.getSimpleName() +
  // ".SHARD_COORDINATOR_IP";
  // public static final String VNODES_ENABLED = ShardRouter.class.getSimpleName() +
  // ".VNODES_ENABLED";
  public static final String IS_WARMUP_QUERY = "query.warmup";
  public static final String TIME_ALLOWED_ENABLE = "timeAllowed.enable";
  public static final String DEFAULT_TIME_ALLOWED_ENABLED_PROPERTY_NAME =
      "dse.timeAllowed.enabled.default";
  public static final boolean DEFAULT_TIME_ALLOWED_ENABLED =
      PropertyConfiguration.getBoolean(DEFAULT_TIME_ALLOWED_ENABLED_PROPERTY_NAME, true);
}
