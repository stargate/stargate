/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.web.docsapi.service.query.search.db.impl;

import com.google.common.collect.ImmutableList;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.query.builder.QueryBuilderImpl;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Simple query builder to get the TTL of a document. */
public class DocumentTtlQueryBuilder extends AbstractSearchQueryBuilder {

  /** Doc id to target. */
  private final String documentId;

  @Override
  protected boolean allowFiltering() {
    return false;
  }

  public DocumentTtlQueryBuilder(String documentId) {
    this.documentId = documentId;
  }

  @Override
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table, String... columns) {
    List<QueryBuilderImpl.FunctionCall> ttlFunction =
        ImmutableList.of(QueryBuilderImpl.FunctionCall.ttl(DocsApiConstants.LEAF_COLUMN_NAME));
    return buildQuery(queryBuilder, keyspace, table, ttlFunction);
  }

  /** {@inheritDoc} */
  @Override
  public Collection<BuiltCondition> getPredicates() {
    return ImmutableList.of(
        BuiltCondition.of(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ, this.documentId));
  }

  @Override
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.emptyMap();
  }
}
