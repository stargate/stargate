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

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import java.util.Collections;
import java.util.List;

/** Simple query builder for document population. */
public final class PopulateSearchQueryBuilder extends AbstractSearchQueryBuilder {

  public PopulateSearchQueryBuilder(DocumentProperties documentProperties) {
    super(documentProperties);
  }

  @Override
  protected boolean allowFiltering() {
    return false;
  }

  @Override
  protected List<BuiltCondition> getPredicates() {
    return Collections.emptyList();
  }

  @Override
  protected List<QueryOuterClass.Value> getValues() {
    return Collections.emptyList();
  }

  @Override
  protected List<BuiltCondition> getBindPredicates() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    BuiltCondition condition =
        BuiltCondition.of(tableProps.keyColumnName(), Predicate.EQ, Term.marker());
    return Collections.singletonList(condition);
  }
}
