/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Simple query parser that places the content of the query string inside the "q" parameter of the
 * Solr request.
 */
public class QQueryParser implements QueryParser {
  @Override
  public ModifiableSolrParams parse(String query) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, query);

    return params;
  }
}
