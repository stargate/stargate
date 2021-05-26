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

package io.stargate.web.docsapi.service.query.search.resolver.impl;

import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.rx.RxUtils;

/** Resolver that does a full search. */
public class FullSearchResolver implements DocumentsResolver {

  private final ExecutionContext context;

  public FullSearchResolver(ExecutionContext parentContext) {
    this.context = parentContext.nested("LoadAllDocuments");
  }

  /** {@inheritDoc} */
  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator) {

    // prepare first (this could be cached for the max depth)
    return RxUtils.singleFromFuture(
            () -> {
              // don't use AbstractSearchQueryBuilder to not have ALLOW_FILTERING on
              DataStore dataStore = queryExecutor.getDataStore();
              String[] columns =
                  QueryConstants.ALL_COLUMNS_NAMES.apply(configuration.getMaxDepth());
              BuiltQuery<? extends AbstractBound<?>> query =
                  dataStore
                      .queryBuilder()
                      .select()
                      .column(columns)
                      .writeTimeColumn(QueryConstants.LEAF_COLUMN_NAME)
                      .from(keyspace, collection)
                      .build();
              return dataStore.prepare(query);
            })
        .cache()
        .flatMapPublisher(
            prepared -> {
              AbstractBound<?> boundQuery = prepared.bind();
              return queryExecutor.queryDocs(
                  boundQuery,
                  configuration.getSearchPageSize(),
                  paginator.getCurrentDbPageState(),
                  context);
            });
  }
}
