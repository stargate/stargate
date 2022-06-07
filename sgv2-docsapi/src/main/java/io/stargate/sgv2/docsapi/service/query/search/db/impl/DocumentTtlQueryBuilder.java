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

package io.stargate.sgv2.docsapi.service.query.search.db.impl;

import com.google.common.collect.ImmutableList;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilderImpl;
import io.stargate.sgv2.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Simple query builder to get the TTL of a document. */
public class DocumentTtlQueryBuilder extends AbstractSearchQueryBuilder {

  public DocumentTtlQueryBuilder(DocumentProperties documentProperties) {
    super(documentProperties);
  }

  @Override
  protected boolean allowFiltering() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public QueryOuterClass.Query buildQuery(String keyspace, String table, String... columns) {
    DocumentTableProperties tableProps = documentProperties.tableProperties();
    List<QueryBuilderImpl.FunctionCall> ttlFunction =
        ImmutableList.of(QueryBuilderImpl.FunctionCall.ttl(tableProps.leafColumnName()));
    return buildQuery(keyspace, table, null, ttlFunction, tableProps.keyColumnName());
  }

  /** {@inheritDoc} */
  @Override
  public Collection<BuiltCondition> getPredicates() {
    return Collections.emptyList();
  }

  /** {@inheritDoc} */
  @Override
  protected Collection<BuiltCondition> getBindPredicates() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    BuiltCondition condition =
        BuiltCondition.of(tableProps.keyColumnName(), Predicate.EQ, Term.marker());
    return Collections.singletonList(condition);
  }
}
