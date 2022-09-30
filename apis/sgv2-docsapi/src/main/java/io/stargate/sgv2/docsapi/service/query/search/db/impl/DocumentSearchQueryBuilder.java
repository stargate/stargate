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

import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The query builder that extends the {@link FilterExpressionSearchQueryBuilder} and adds predicate
 * to match the document id. Binding of the document id is needed for the query provided.
 */
public class DocumentSearchQueryBuilder extends FilterExpressionSearchQueryBuilder {

  public DocumentSearchQueryBuilder(
      DocumentProperties documentProperties, FilterExpression expression) {
    this(documentProperties, Collections.singleton(expression));
  }

  public DocumentSearchQueryBuilder(
      DocumentProperties documentProperties, Collection<FilterExpression> expressions) {
    super(documentProperties, expressions);
  }

  @Override
  protected List<BuiltCondition> getBindPredicates() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    List<BuiltCondition> bindPredicates = new ArrayList<>(super.getBindPredicates());
    BuiltCondition condition =
        BuiltCondition.of(tableProps.keyColumnName(), Predicate.EQ, Term.marker());
    bindPredicates.add(condition);
    return bindPredicates;
  }
}
