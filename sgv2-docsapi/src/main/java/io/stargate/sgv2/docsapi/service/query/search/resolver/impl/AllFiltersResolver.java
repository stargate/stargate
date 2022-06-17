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

package io.stargate.sgv2.docsapi.service.query.search.resolver.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AllFiltersResolver extends AbstractFiltersResolver {

  private final Collection<CandidatesFilter> candidatesFilters;

  private final DocumentsResolver candidatesResolver;

  public AllFiltersResolver(
      Function<ExecutionContext, CandidatesFilter> candidatesFilterSupplier,
      ExecutionContext context,
      DocumentsResolver candidatesResolver) {
    this(Collections.singleton(candidatesFilterSupplier), context, candidatesResolver);
  }

  public AllFiltersResolver(
      Collection<Function<ExecutionContext, CandidatesFilter>> candidatesFilterSuppliers,
      ExecutionContext context,
      DocumentsResolver candidatesResolver) {
    ExecutionContext nested = context.nested("PARALLEL [ALL OF]");
    this.candidatesFilters =
        candidatesFilterSuppliers.stream().map(s -> s.apply(nested)).collect(Collectors.toList());
    this.candidatesResolver = candidatesResolver;
  }

  /** {@inheritDoc} */
  @Override
  protected Collection<CandidatesFilter> getCandidatesFilters() {
    return candidatesFilters;
  }

  /** {@inheritDoc} */
  @Override
  protected DocumentsResolver getCandidatesResolver() {
    return candidatesResolver;
  }

  /** {@inheritDoc} */
  @Override
  protected Uni<Boolean> resolveSources(List<Uni<Boolean>> sources) {
    // only if all filters emit true, return the doc
    // this means all filters are passed

    return Uni.join().all(sources).andFailFast().map(results -> !results.contains(Boolean.FALSE));
  }
}
