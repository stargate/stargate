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

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** The search query builder that creates all needed predicates for a {@link FilterPath}. */
public class FilterPathSearchQueryBuilder extends PathSearchQueryBuilder {

  private final FilterPath filterPath;

  private final boolean matchField;

  /**
   * @param filterPath Filter path
   * @param matchField If field name should be matches as well, adds extra predicates
   */
  public FilterPathSearchQueryBuilder(FilterPath filterPath, boolean matchField) {
    super(filterPath.getParentPath());
    this.filterPath = filterPath;
    this.matchField = matchField;
  }

  @Override
  protected boolean allowFiltering() {
    return true;
  }

  @Override
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.emptyMap();
  }

  @Override
  public Collection<BuiltCondition> getPredicates() {
    Collection<BuiltCondition> predicates = super.getPredicates();
    predicates.addAll(getFieldPredicates());
    predicates.addAll(getRemainingPathPredicates());
    return predicates;
  }

  public FilterPath getFilterPath() {
    return filterPath;
  }

  private List<BuiltCondition> getFieldPredicates() {
    int parentSize = filterPath.getParentPath().size();

    // TODO convert to config read
    if (parentSize >= DocumentDB.MAX_DEPTH) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    } else {
      String field = DocsApiUtils.convertEscapedCharacters(filterPath.getField());
      if (matchField) {
        // apply to both p and leaf, as index is on leaf and we want it kicking in
        return Arrays.asList(
            BuiltCondition.of(QueryConstants.P_COLUMN_NAME.apply(parentSize), Predicate.EQ, field),
            BuiltCondition.of(QueryConstants.LEAF_COLUMN_NAME, Predicate.EQ, field));
      } else {
        // TODO confirm this is really needed
        //  confirm this could be needed only on non-empty path
        return Collections.singletonList(
            BuiltCondition.of(QueryConstants.P_COLUMN_NAME.apply(parentSize), Predicate.GT, ""));
      }
    }
  }

  private List<BuiltCondition> getRemainingPathPredicates() {
    int fullSize = filterPath.getPath().size();
    if (fullSize >= DocumentDB.MAX_DEPTH) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(
          BuiltCondition.of(QueryConstants.P_COLUMN_NAME.apply(fullSize), Predicate.EQ, ""));
    }
  }
}
