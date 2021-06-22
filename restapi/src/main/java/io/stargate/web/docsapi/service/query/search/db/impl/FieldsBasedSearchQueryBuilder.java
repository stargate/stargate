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
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Query builder that can optimize when field selection is known. Note that this does not support */
public class FieldsBasedSearchQueryBuilder extends AbstractSearchQueryBuilder {

  private final FieldsOptimization fieldsOptimization;

  public FieldsBasedSearchQueryBuilder(List<String[]> fieldPaths) {
    if (null == fieldPaths) {
      this.fieldsOptimization = new FieldsOptimization(Collections.emptyList());
    } else {
      this.fieldsOptimization = new FieldsOptimization(fieldPaths);
    }
  }

  @Override
  protected boolean allowFiltering() {
    return fieldsOptimization.canOptimize();
  }

  @Override
  protected Collection<BuiltCondition> getPredicates() {
    if (!fieldsOptimization.canOptimize()) {
        return Collections.emptyList();
    }

    List<BuiltCondition> predicates = new ArrayList<>();
    List<String> commonPrefix = fieldsOptimization.commonPrefix;
    for (int i = 0; i < fieldsOptimization.minDepth; i++) {
      BuiltCondition condition;
      String column = QueryConstants.P_COLUMN_NAME.apply(i);
      if (i < commonPrefix.size()) {
        condition = BuiltCondition.of(column, Predicate.EQ, commonPrefix.get(i));
      } else {
        condition = BuiltCondition.of(column, Predicate.GT, "");
      }
      predicates.add(condition);
    }
    return predicates;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.emptyMap();
  }

  // converts the field paths to the optimization pojo
  private static class FieldsOptimization {

    final int minDepth;
    final List<String> commonPrefix;

    public FieldsOptimization(List<String[]> fieldPaths) {
      minDepth = fieldPaths.stream()
              .mapToInt(strings -> strings.length)
              .min()
              .orElse(0);

      this.commonPrefix = new ArrayList<>();
      for (int i = 0; i < minDepth; i++) {
        int index = i;
        List<String> pathsAtIndex = fieldPaths.stream()
                .map(strings -> strings[index])
                .distinct()
                .collect(Collectors.toList());

        if (pathsAtIndex.size() != 1) {
          break;
        } else {
          commonPrefix.addAll(pathsAtIndex);
        }
      }
    }

    boolean canOptimize() {
      return minDepth > 1 || !commonPrefix.isEmpty();
    }

  }

}
