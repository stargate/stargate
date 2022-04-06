/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import static com.datastax.bdp.search.solr.dht.ShardRouterConstants.TIME_ALLOWED_ENABLE;

import com.datastax.bdp.search.solr.dht.ShardRouterConstants;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;

/**
 * Abstract parser for JSON queries. Each parameter is parsed separately, so each implementation can
 * have its own syntax.
 */
public abstract class AbstractJSONQueryParser implements QueryParser {
  @VisibleForTesting public static final ObjectMapper JSON_MAPPER;
  // TODO-SEARCH
  public static final String QUERY_NAME = "query.name";

  private static final Set<String> SUPPORTED_PARAMETERS =
      ImmutableSet.of(
          CommonParams.Q,
          CommonParams.FQ,
          CommonParams.SORT,
          CommonParams.START,
          CommonParams.TIME_ALLOWED,
          CommonParams.DOCID_SORTED_SEGMENT_TERMINATE_EARLY,
          CommonParams.TZ,
          FacetParams.FACET,
          CqlSolrQueryRequest.COMMIT,
          CqlSolrQueryRequest.PAGING,
          CqlSolrQueryRequest.USE_FIELD_CACHE,
          QUERY_NAME,
          ShardRouterConstants.ROUTE_BY_PARTITION,
          ShardRouterConstants.ROUTE_BY_RANGE,
          ShardParams.SHARDS_FAILOVER,
          ShardParams.SHARDS_TOLERANT,
          ShardParams.DISTRIB_SINGLE_PASS,
          TIME_ALLOWED_ENABLE);

  static {
    JSON_MAPPER = new ObjectMapper();
    JSON_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ModifiableSolrParams parse(String query) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    try {
      MultiValueMap json;

      try {
        json = JSON_MAPPER.readValue(query, MultiValueMap.class);
      } catch (JsonProcessingException ex) {
        throw new IllegalArgumentException("Cannot parse JSON query: " + query);
      }

      for (Object parameter : json.keySet()) {
        if (!SUPPORTED_PARAMETERS.contains(parameter.toString())) {
          throw new IllegalArgumentException(
              "Unsupported query parameter: " + parameter.toString());
        }
      }

      Object q = getFirstItemOrNull(json, CommonParams.Q);
      if (q != null) {
        if (((Collection<Object>) json.get(CommonParams.Q)).size() > 1) {
          throw new IllegalArgumentException(
              "Parameter [" + CommonParams.Q + "] can only be specified once.");
        }
        parseQuery(CommonParams.Q, q, params);
      } else {
        throw new IllegalArgumentException(
            "Query must contain at least the \"q\" parameter, found: " + query);
      }

      // allow multiple FQs, may be a Collection<String> or a Collection<Collection<String>>
      Collection<Object> fqs = (Collection<Object>) json.get(CommonParams.FQ);
      if (fqs != null) {
        List<Object> fqList = new ArrayList();
        for (Object o : fqs) {
          if (o instanceof Collection) {
            fqList.addAll((Collection) o);
          } else {
            fqList.add(o);
          }
        }
        parseFilter(CommonParams.FQ, fqList, params);
      }

      Object sort = getFirstItemOrNull(json, CommonParams.SORT);
      if (sort != null) {
        parseSort(CommonParams.SORT, sort, params);
      }

      Object start = getFirstItemOrNull(json, CommonParams.START);
      if (start != null) {
        parseStart(CommonParams.START, start, params);
      }

      Object timeAllowed = getFirstItemOrNull(json, CommonParams.TIME_ALLOWED);
      if (timeAllowed != null) {
        parseTimeAllowed(CommonParams.TIME_ALLOWED, timeAllowed, params);
      }

      Object terminateEarly =
          getFirstItemOrNull(json, CommonParams.DOCID_SORTED_SEGMENT_TERMINATE_EARLY);
      if (terminateEarly != null) {
        parseTerminateEarly(
            CommonParams.DOCID_SORTED_SEGMENT_TERMINATE_EARLY, terminateEarly, params);
      }

      Object timeAllowedEnable = getFirstItemOrNull(json, TIME_ALLOWED_ENABLE);
      if (timeAllowedEnable != null) {
        parseTimeAllowedEnable(TIME_ALLOWED_ENABLE, timeAllowedEnable, params);
      }

      Object timeZone = getFirstItemOrNull(json, CommonParams.TZ);
      if (timeZone != null) {
        parseTimeZone(CommonParams.TZ, timeZone, params);
      }

      Object solrPaging = getFirstItemOrNull(json, CqlSolrQueryRequest.PAGING);
      if (solrPaging != null) {
        parsePaging(CqlSolrQueryRequest.PAGING, solrPaging, params);
      }

      Object facet = getFirstItemOrNull(json, FacetParams.FACET);
      if (facet != null) {
        parseFacet(FacetParams.FACET, facet, params);
      }

      Object commit = getFirstItemOrNull(json, CqlSolrQueryRequest.COMMIT);
      if (commit != null) {
        parseCommit(CqlSolrQueryRequest.COMMIT, commit, params);
      }

      Object queryName = getFirstItemOrNull(json, QUERY_NAME);
      if (queryName != null) {
        parseQueryName(QUERY_NAME, queryName, params);
      }

      Object routePartition = getFirstItemOrNull(json, ShardRouterConstants.ROUTE_BY_PARTITION);
      if (routePartition != null) {
        parseRoutePartition(ShardRouterConstants.ROUTE_BY_PARTITION, routePartition, params);
      }
      Object routeRange = getFirstItemOrNull(json, ShardRouterConstants.ROUTE_BY_RANGE);
      if (routeRange != null) {
        parseRouteRange(ShardRouterConstants.ROUTE_BY_RANGE, routeRange, params);
      }
      if (routePartition != null && routeRange != null) {
        throw new IllegalArgumentException(
            String.format(
                "Only one of %s or %s can be specified!",
                ShardRouterConstants.ROUTE_BY_PARTITION, ShardRouterConstants.ROUTE_BY_RANGE));
      }

      Object shardsFailover = getFirstItemOrNull(json, ShardParams.SHARDS_FAILOVER);
      if (shardsFailover != null) {
        parseShardsFailover(ShardParams.SHARDS_FAILOVER, shardsFailover, params);
      }

      Object shardsTolerant = getFirstItemOrNull(json, ShardParams.SHARDS_TOLERANT);
      if (shardsTolerant != null) {
        parseShardsTolerant(ShardParams.SHARDS_TOLERANT, shardsTolerant, params);
      }

      Object singlePass = getFirstItemOrNull(json, ShardParams.DISTRIB_SINGLE_PASS);
      if (singlePass != null) {
        parseSinglePass(ShardParams.DISTRIB_SINGLE_PASS, singlePass, params);
      }

      Object useFieldCache = getFirstItemOrNull(json, CqlSolrQueryRequest.USE_FIELD_CACHE);
      if (useFieldCache != null) {
        parseUseFieldCache(CqlSolrQueryRequest.USE_FIELD_CACHE, useFieldCache, params);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    return params;
  }

  @SuppressWarnings("unchecked")
  protected <T> T ensureType(String param, Object obj, Class<T> clazz) {
    if (obj != null) {
      if (clazz.isInstance(obj)) {
        return (T) obj;
      } else {
        throw new IllegalArgumentException(
            "Expected " + clazz + " but got " + obj.getClass() + " for parameter " + param);
      }
    } else {
      return null;
    }
  }

  protected Class<?> findType(String param, Object obj, Class<?>... classes) {
    for (Class<?> current : classes) {
      try {
        ensureType(param, obj, current);
        return current;
      } catch (IllegalArgumentException e) {
      }
    }

    // If we get here none of the supplied classes are compatible
    throw new IllegalArgumentException(
        "Expected one of "
            + Arrays.toString(classes)
            + " but got "
            + obj.getClass()
            + " for parameter "
            + param);
  }

  protected abstract void parseQuery(String param, Object query, ModifiableSolrParams params);

  protected abstract void parseFilter(String param, Object filter, ModifiableSolrParams params);

  protected abstract void parseSort(String param, Object sort, ModifiableSolrParams params);

  protected abstract void parseStart(String param, Object start, ModifiableSolrParams params);

  protected abstract void parseTimeAllowed(
      String param, Object timeAllowed, ModifiableSolrParams params);

  protected abstract void parseTerminateEarly(
      String param, Object timeAllowed, ModifiableSolrParams params);

  protected abstract void parseTimeAllowedEnable(
      String param, Object timeAllowedEnable, ModifiableSolrParams params);

  protected abstract void parseTimeZone(String param, Object timeZone, ModifiableSolrParams params);

  protected abstract void parsePaging(String param, Object start, ModifiableSolrParams params);

  protected abstract void parseFacet(String param, Object facet, ModifiableSolrParams params);

  protected abstract void parseCommit(String param, Object commit, ModifiableSolrParams params);

  protected abstract void parseQueryName(
      String param, Object queryName, ModifiableSolrParams params);

  protected abstract void parseRoutePartition(
      String param, Object routePartition, ModifiableSolrParams params);

  protected abstract void parseRouteRange(
      String param, Object routeRange, ModifiableSolrParams params);

  protected abstract void parseShardsFailover(
      String param, Object shardsFailover, ModifiableSolrParams params);

  protected abstract void parseShardsTolerant(
      String param, Object shardsTolerant, ModifiableSolrParams params);

  protected abstract void parseSinglePass(
      String param, Object singlePass, ModifiableSolrParams params);

  protected abstract void parseUseFieldCache(
      String param, Object useFieldCache, ModifiableSolrParams params);

  @SuppressWarnings("unchecked")
  private Object getFirstItemOrNull(MultiValueMap multimap, String key) {
    Collection<Object> co = (Collection<Object>) multimap.get(key);
    if (co == null || co.isEmpty()) {
      return null;
    } else {
      return co.iterator().next();
    }
  }
}
