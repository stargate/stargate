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

package io.stargate.sgv2.docsapi.service.query.search.resolver.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractFiltersResolver implements DocumentsResolver {

  /**
   * @return Resolver used to fetch candidates.
   */
  protected abstract DocumentsResolver getCandidatesResolver();

  /**
   * @return Collection of the {@link CandidatesFilter}s that should be used for filtering
   *     candidates.
   */
  protected abstract Collection<CandidatesFilter> getCandidatesFilters();

  /**
   * Resolves the given filter results based on the type of the resolver into a single Uni.
   *
   * @param sources All filter results
   * @return Uni that emits true if the document should be selected
   */
  protected abstract Uni<Boolean> resolveSources(List<Uni<Boolean>> sources);

  @Override
  public Multi<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {
    Multi<RawDocument> candidates =
        getCandidatesResolver().getDocuments(queryExecutor, keyspace, collection, paginator);

    Uni<List<Pair<QueryOuterClass.Query, CandidatesFilter>>> queriesToCandidates =
        Multi.createFrom()
            .iterable(getCandidatesFilters())
            .onItem()
            .transformToUniAndMerge(
                filter ->
                    filter.prepareQuery(keyspace, collection).map(built -> Pair.of(built, filter)))

            // collect as list
            .collect()
            .asList()

            // cache
            .memoize()
            .indefinitely();

    return candidates

        // keep the order of incoming docs
        .onItem()
        .transformToUniAndConcatenate(
            doc -> queriesToCandidates.map(prepared -> Pair.of(doc, prepared)))

        // keep the order of the resolved docs
        .onItem()
        .transformToMultiAndConcatenate(
            pair -> {
              RawDocument doc = pair.getLeft();
              List<Uni<Boolean>> sources =
                  pair.getRight().stream()
                      .map(
                          queryToFilter -> {
                            CandidatesFilter filter = queryToFilter.getRight();
                            QueryOuterClass.Query query = queryToFilter.getLeft();
                            return filter.bindAndFilter(queryExecutor, query, doc);
                          })
                      .collect(Collectors.toList());

              // only if resolving filters emits an item, return the doc
              return resolveSources(sources)

                  // only emit the document if the result of resolving is true
                  .onItem()
                  .transformToMulti(
                      result -> {
                        if (Boolean.TRUE.equals(result)) {
                          return Multi.createFrom().items(doc);
                        } else {
                          return Multi.createFrom().empty();
                        }
                      });
            });
  }
}
