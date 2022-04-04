/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;

/** {@link SolrQueryRequest} implementation for CQL Solr queries. */
public class CqlSolrQueryRequest extends SolrQueryRequestBase {
  public static final String COMMIT = "commit";
  public static final String PAGING = "paging";
  public static final String USE_FIELD_CACHE = "useFieldCache";
  public final boolean commit;
  public final boolean tolerant;

  public CqlSolrQueryRequest(SolrCore core, SolrParams params) {
    super(core, params);
    this.commit = Boolean.parseBoolean(params.get(COMMIT));
    this.tolerant = Boolean.parseBoolean(params.get(ShardParams.SHARDS_TOLERANT));
  }
}
