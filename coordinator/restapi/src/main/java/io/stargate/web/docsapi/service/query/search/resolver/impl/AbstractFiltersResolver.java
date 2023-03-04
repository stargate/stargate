/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in comHpliance with the License.
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
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractFiltersResolver implements DocumentsResolver {

  protected abstract DocumentsResolver getCandidatesResolver();

  protected abstract Collection<CandidatesFilter> getCandidatesFilters();

  protected abstract Flowable<RawDocument> resolveSources(
      RawDocument rawDocument, List<Maybe<?>> sources);

  protected DocsApiConfiguration config;

  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {
    Flowable<RawDocument> candidates =
        getCandidatesResolver().getDocuments(queryExecutor, keyspace, collection, paginator);

    Single<List<Pair<? extends Query<? extends BoundQuery>, CandidatesFilter>>>
        queriesToCandidates =
            Flowable.fromIterable(getCandidatesFilters())
                .flatMap(
                    filter -> {
                      Single<Pair<? extends Query<? extends BoundQuery>, CandidatesFilter>>
                          pairSingle =
                              filter
                                  .prepareQuery(queryExecutor.getDataStore(), keyspace, collection)
                                  .zipWith(Single.just(filter), Pair::of);

                      return pairSingle.toFlowable();
                    })
                .toList()
                .cache();

    return candidates
        .concatMapSingle(doc -> queriesToCandidates.map(prepared -> Pair.of(doc, prepared)))
        .concatMap(
            pair -> {
              RawDocument doc = pair.getLeft();
              List<Maybe<?>> sources =
                  pair.getRight().stream()
                      .map(
                          queryToFilter -> {
                            CandidatesFilter filter = queryToFilter.getRight();
                            Query<? extends BoundQuery> query = queryToFilter.getLeft();
                            return filter.bindAndFilter(queryExecutor, query, doc);
                          })
                      .collect(Collectors.toList());

              // only if all filters emit the item, return the doc
              // this means all filters are passed
              return resolveSources(doc, sources);
            });
  }
}
