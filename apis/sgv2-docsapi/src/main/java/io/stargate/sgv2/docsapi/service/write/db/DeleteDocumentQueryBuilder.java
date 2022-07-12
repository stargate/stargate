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

package io.stargate.sgv2.docsapi.service.write.db;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import java.util.List;

/**
 * Delete query builder that deletes a complete document.
 *
 * <p>For example, given the document:
 *
 * <pre>
 * { "a": 1, "b": 2 }
 * </pre>
 *
 * That gets stored as the following rows:
 *
 * <pre>
 * key | p0 | p1 | p2 | dbl_value
 * --- +----+----+----+-----------
 * xyz |  a |    |    |         1
 * xyz |  b |    |    |         2
 * </pre>
 *
 * The following DELETE conditions will be generated:
 *
 * <pre>
 * WHERE key = 'xyz'
 * </pre>
 */
public class DeleteDocumentQueryBuilder extends AbstractDeleteQueryBuilder {

  public DeleteDocumentQueryBuilder(DocumentProperties documentProperties) {
    super(documentProperties);
  }

  @Override
  protected void addWhereConditions(List<BuiltCondition> whereConditions) {
    // intentionally empty
  }

  @Override
  protected void addBindValues(List<QueryOuterClass.Value> values) {
    // intentionally empty
  }
}
