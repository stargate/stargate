/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import org.apache.solr.common.params.ModifiableSolrParams;

/** Parser for CQL Solr queries, producing a valid Solr query request. */
public interface QueryParser {
  ModifiableSolrParams parse(String query);
}
