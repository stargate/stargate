/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Simple JSON query parser with pure string representation of each element/parameter, except:
 *
 * <ul>
 *   <li>"start" represented as number.
 *   <li>"commit" represented as boolean.
 *   <li>"route.partition" represented as string array
 *   <li>"route.range" represented as string array
 * </ul>
 */
public class SimpleJSONQueryParser extends AbstractJSONQueryParser {
  private static final char COMMA = ',';
  private static final char DOT = '.';
  private static final Joiner COMMA_JOINER = Joiner.on(COMMA);
  private static final Joiner DOT_JOINER = Joiner.on(DOT);
  private static final Splitter DOT_SPLITTER = Splitter.on(DOT);

  @Override
  @VisibleForTesting
  public void parseQuery(String param, Object query, ModifiableSolrParams params) {
    ensureType(param, query, String.class);
    params.add(param, query.toString());
  }

  @Override
  @VisibleForTesting
  public void parseFilter(String param, Object filter, ModifiableSolrParams params) {
    Class<?> compatibleClass = findType(param, filter, String.class, List.class);

    if (compatibleClass.equals(List.class)) {
      for (String current : (List<String>) filter) {
        params.add(param, current);
      }
    } else {
      params.add(param, filter.toString());
    }
  }

  @Override
  @VisibleForTesting
  public void parseSort(String param, Object sort, ModifiableSolrParams params) {
    ensureType(param, sort, String.class);
    params.add(param, sort.toString());
  }

  @Override
  @VisibleForTesting
  public void parsePaging(String param, Object solrPaging, ModifiableSolrParams params) {
    ensureType(param, solrPaging, String.class);
    params.add(param, solrPaging.toString());
  }

  @Override
  @VisibleForTesting
  public void parseStart(String param, Object start, ModifiableSolrParams params) {
    findType(param, start, String.class, Number.class);
    params.add(param, start.toString());
  }

  @Override
  @VisibleForTesting
  public void parseTimeAllowed(String param, Object timeAllowed, ModifiableSolrParams params) {
    ensureType(param, timeAllowed, Number.class);
    params.add(param, timeAllowed.toString());
  }

  @Override
  @VisibleForTesting
  public void parseTerminateEarly(
      String param, Object terminateEarly, ModifiableSolrParams params) {
    Class<?> compatibleClass = findType(param, terminateEarly, String.class, Boolean.class);
    if (compatibleClass.equals(String.class)) {
      params.add(param, (String) terminateEarly);
    } else if (compatibleClass.equals(Boolean.class)) {
      params.add(param, terminateEarly.toString());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "The format for property '%s' is incorrect. Use either true or false instead.",
              param));
    }
  }

  public void parseTimeAllowedEnable(
      String param, Object timeAllowedEnable, ModifiableSolrParams params) {
    ensureType(param, timeAllowedEnable, Boolean.class);
    params.add(param, timeAllowedEnable.toString());
  }

  @Override
  @VisibleForTesting
  public void parseTimeZone(String param, Object timeZone, ModifiableSolrParams params) {
    ensureType(param, timeZone, String.class);
    params.add(param, timeZone.toString());
  }

  @Override
  @VisibleForTesting
  public void parseFacet(String param, Object facet, ModifiableSolrParams params) {
    ensureType(param, facet, Map.class);
    params.add(param, Boolean.TRUE.toString());
    for (Entry entry : ((Map<Object, Object>) facet).entrySet()) {
      // Rebuild the key (the facet parameter) by inserting the "facet" prefix either after the
      // field name or at the very beginning:
      String key = entry.getKey().toString();
      List<String> parts = Lists.newArrayList(DOT_SPLITTER.split(key));
      if (parts.get(0).equals("facet")) {
        parts.remove(0);
        throw new IllegalArgumentException(
            String.format(
                "The format for property '%s' is incorrect. Use '%s' instead.",
                key, DOT_JOINER.join(parts)));
      } else if (parts.get(0).equals("f")) {
        if (!Strings.isNullOrEmpty(parts.get(2)) && parts.get(2).equals("facet")) {
          parts.remove(2);
          throw new IllegalArgumentException(
              String.format(
                  "The format for property '%s' is incorrect. Use '%s' instead.",
                  key, DOT_JOINER.join(parts)));
        }
        parts.add(2, "facet");
      } else {
        parts.add(0, "facet");
      }
      key = DOT_JOINER.join(parts);

      // Add values corresponding to the key:
      Object value = entry.getValue();
      if (value instanceof List) {
        List<Object> values = (List<Object>) value;
        for (Object single : values) {
          params.add(key, single.toString());
        }
      } else {
        params.add(key, value.toString());
      }
    }
  }

  @Override
  @VisibleForTesting
  public void parseCommit(String param, Object commit, ModifiableSolrParams params) {
    ensureType(param, commit, Boolean.class);
    params.add(param, commit.toString());
  }

  @Override
  @VisibleForTesting
  public void parseQueryName(String param, Object queryName, ModifiableSolrParams params) {
    ensureType(param, queryName, String.class);
    params.add(param, queryName.toString());
  }

  @Override
  @VisibleForTesting
  public void parseRoutePartition(
      String param, Object routePartition, ModifiableSolrParams params) {
    ensureType(param, routePartition, List.class);
    List input = (List) routePartition;
    params.add(param, COMMA_JOINER.join(input));
  }

  @Override
  @VisibleForTesting
  public void parseRouteRange(String param, Object routeRange, ModifiableSolrParams params) {
    ensureType(param, routeRange, List.class);
    List input = (List) routeRange;
    params.add(param, COMMA_JOINER.join(input));
  }

  @Override
  @VisibleForTesting
  public void parseShardsFailover(
      String param, Object shardsFailover, ModifiableSolrParams params) {
    ensureType(param, shardsFailover, Boolean.class);
    params.add(param, shardsFailover.toString());
  }

  @Override
  @VisibleForTesting
  public void parseShardsTolerant(
      String param, Object shardsTolerant, ModifiableSolrParams params) {
    ensureType(param, shardsTolerant, Boolean.class);
    params.add(param, shardsTolerant.toString());
  }

  @Override
  @VisibleForTesting
  public void parseSinglePass(String param, Object singlePass, ModifiableSolrParams params) {
    ensureType(param, singlePass, Boolean.class);
    params.add(param, singlePass.toString());
  }

  @Override
  @VisibleForTesting
  public void parseUseFieldCache(String param, Object useFieldCache, ModifiableSolrParams params) {
    ensureType(param, useFieldCache, Boolean.class);
    params.add(param, useFieldCache.toString());
  }
}
