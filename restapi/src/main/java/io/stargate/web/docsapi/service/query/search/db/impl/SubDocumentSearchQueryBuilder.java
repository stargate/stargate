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
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.Collection;
import java.util.List;

/**
 * Search query builder that matches all rows on the given #subDocumentsPath for a single document.
 */
public class SubDocumentSearchQueryBuilder extends PathSearchQueryBuilder {

  /** Doc id to target. */
  private final String documentId;

  public SubDocumentSearchQueryBuilder(String documentId, List<String> subDocumentsPath) {
    super(subDocumentsPath);
    this.documentId = documentId;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<BuiltCondition> getPredicates() {
    Collection<BuiltCondition> predicates = super.getPredicates();
    predicates.add(BuiltCondition.of(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ, documentId));
    return predicates;
  }
}
