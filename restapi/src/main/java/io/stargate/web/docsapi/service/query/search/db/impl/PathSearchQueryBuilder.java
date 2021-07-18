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
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The search query builder that creates all needed predicates for a path represented as a list of
 * string.
 */
public class PathSearchQueryBuilder extends AbstractSearchQueryBuilder {

  private final List<String> path;

  /** @param path Path to match. */
  public PathSearchQueryBuilder(List<String> path) {
    this.path = path;
  }

  @Override
  protected boolean allowFiltering() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.emptyMap();
  }

  /** {@inheritDoc} */
  @Override
  public Collection<BuiltCondition> getPredicates() {
    return getPathPredicates();
  }

  private List<BuiltCondition> getPathPredicates() {
    List<BuiltCondition> predicates = new ArrayList<>();

    // copied from the DocumentService
    for (int i = 0; i < path.size(); i++) {
      String next = path.get(i);
      String[] pathSegmentSplit = next.split(",");
      if (pathSegmentSplit.length == 1) {
        String pathSegment = pathSegmentSplit[0];
        if (pathSegment.equals(DocumentDB.GLOB_VALUE)
            || pathSegment.equals(DocumentDB.GLOB_ARRAY_VALUE)) {
          predicates.add(
              BuiltCondition.of(QueryConstants.P_COLUMN_NAME.apply(i), Predicate.GT, ""));
        } else {
          predicates.add(
              BuiltCondition.of(QueryConstants.P_COLUMN_NAME.apply(i), Predicate.EQ, pathSegment));
        }
      } else {
        predicates.add(
            BuiltCondition.of(
                QueryConstants.P_COLUMN_NAME.apply(i),
                Predicate.IN,
                Arrays.asList(pathSegmentSplit)));
      }
    }

    return predicates;
  }
}
