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
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The query builder that extends the {@link FilterExpressionSearchQueryBuilder} and adds predicate
 * to match the document id. Binding of the document id is needed for the query provided.
 */
public class DocumentSearchQueryBuilder extends FilterExpressionSearchQueryBuilder {

  public DocumentSearchQueryBuilder(FilterExpression expression, DocsApiConfiguration config) {
    this(Collections.singleton(expression), config);
  }

  public DocumentSearchQueryBuilder(
      Collection<FilterExpression> expressions, DocsApiConfiguration config) {
    super(expressions, config);
  }

  @Override
  protected Map<String, Predicate> getBindPredicates() {
    Map<String, Predicate> bindPredicates = new HashMap<>(1);
    bindPredicates.put(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ);
    bindPredicates.putAll(super.getBindPredicates());
    return bindPredicates;
  }
}
