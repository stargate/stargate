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

import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** The search query builder that creates all needed predicates for a {@link FilterPath}. */
public class FilterPathSearchQueryBuilder extends PathSearchQueryBuilder {

  private final FilterPath filterPath;

  private final boolean matchField;

  /**
   * @param filterPath Filter path
   * @param matchField If field name should be matches as well, adds extra predicates
   */
  public FilterPathSearchQueryBuilder(
      DocumentProperties documentProperties, FilterPath filterPath, boolean matchField) {
    super(documentProperties, filterPath.getParentPath());
    this.filterPath = filterPath;
    this.matchField = matchField;
  }

  @Override
  protected boolean allowFiltering() {
    return true;
  }

  @Override
  protected Collection<BuiltCondition> getBindPredicates() {
    return Collections.emptyList();
  }

  @Override
  public Collection<BuiltCondition> getPredicates() {
    Collection<BuiltCondition> predicates = super.getPredicates();

    int maxDepth = documentProperties.maxDepth();
    predicates.addAll(getFieldPredicates(maxDepth));
    predicates.addAll(getRemainingPathPredicates(maxDepth));
    return predicates;
  }

  public FilterPath getFilterPath() {
    return filterPath;
  }

  private List<BuiltCondition> getFieldPredicates(int maxDepth) {
    int parentSize = filterPath.getParentPath().size();
    if (parentSize >= maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    } else {
      DocumentTableProperties tableProps = documentProperties.tableProperties();
      String field = DocsApiUtils.convertEscapedCharacters(filterPath.getField());
      if (matchField) {
        // apply to p and leaf, as index is on leaf, and we want it kicking in
        return Arrays.asList(
            BuiltCondition.of(
                tableProps.pathColumnName(parentSize), Predicate.EQ, Values.of(field)),
            BuiltCondition.of(tableProps.leafColumnName(), Predicate.EQ, Values.of(field)));
      } else {
        // TODO confirm this is really needed
        //  confirm this could be needed only on non-empty path
        return Collections.singletonList(
            BuiltCondition.of(tableProps.pathColumnName(parentSize), Predicate.GT, Values.of("")));
      }
    }
  }

  private List<BuiltCondition> getRemainingPathPredicates(int maxDepth) {
    int fullSize = filterPath.getPath().size();
    if (fullSize >= maxDepth) {
      return Collections.emptyList();
    } else {
      DocumentTableProperties tableProps = documentProperties.tableProperties();
      return Collections.singletonList(
          BuiltCondition.of(tableProps.pathColumnName(fullSize), Predicate.EQ, Values.of("")));
    }
  }
}
