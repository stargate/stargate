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
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Search query builder that matches all rows on the given #subDocumentsPath for a single document.
 */
public class SubDocumentSearchQueryBuilder extends PathSearchQueryBuilder {

  /** Doc id to target. */
  private final String documentId;

  public SubDocumentSearchQueryBuilder(
      DocumentProperties documentProperties, String documentId, List<String> subDocumentsPath) {
    super(documentProperties, subDocumentsPath);
    this.documentId = documentId;
  }

  @Override
  protected Pair<List<BuiltCondition>, List<QueryOuterClass.Value>> resolve() {
    Pair<List<BuiltCondition>, List<QueryOuterClass.Value>> resolve = super.resolve();

    List<BuiltCondition> predicates = resolve.getLeft();
    List<QueryOuterClass.Value> values = resolve.getRight();

    DocumentTableProperties tableProps = documentProperties.tableProperties();
    predicates.add(BuiltCondition.of(tableProps.keyColumnName(), Predicate.EQ, Term.marker()));
    values.add(Values.of(documentId));

    return Pair.of(predicates, values);
  }
}
