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

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.List;

/**
 * Deletes any array elements in the given document sub-path.
 *
 * <p>For example, given the document:
 *
 * <pre>
 * { "a": 1, "b": [2, 3] }
 * </pre>
 *
 * That gets stored as the following rows:
 *
 * <pre>
 * key | p0 | p1       | p2 | dbl_value
 * --- +----+----------+----+-----------
 * xyz |  a |          |    |         1
 * xyz |  b | [000000] |    |         2
 * xyz |  b | [000001] |    |         3
 * </pre>
 *
 * The subpath ["b"] will generate the DELETE conditions:
 *
 * <pre>
 * WHERE key = 'xyz'
 *   AND p0 = 'b'
 *   AND p1 >= '[000000]'
 *   AND p1 <= '[999999]'
 * </pre>
 */
public class DeleteSubDocumentArrayQueryBuilder extends DeleteSubDocumentPathQueryBuilder {

  /**
   * Constructs the query builder for deleting sub-document rows that represent array values.
   *
   * @param subDocumentPath The sub-document path that contains the elements
   */
  public DeleteSubDocumentArrayQueryBuilder(
      List<String> subDocumentPath, DocumentProperties documentProperties) {
    super(subDocumentPath, false, documentProperties);
  }

  @Override
  protected void addWhereConditions(List<BuiltCondition> whereConditions) {
    super.addWhereConditions(whereConditions);

    int subPathSize = subDocumentPath.size();
    if (subPathSize + 1 > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    String targetColumns = documentProperties.tableProperties().pathColumnName(subPathSize);
    whereConditions.add(BuiltCondition.of(targetColumns, Predicate.GTE, Term.marker()));
    whereConditions.add(BuiltCondition.of(targetColumns, Predicate.LTE, Term.marker()));
  }

  @Override
  protected void addBindValues(List<QueryOuterClass.Value> values) {
    super.addBindValues(values);

    String beginIndex = DocsApiUtils.convertArrayPath("[0]", documentProperties.maxArrayLength());
    String endIndex = String.format("[%d]", documentProperties.maxArrayLength() - 1);

    values.add(Values.of(beginIndex));
    values.add(Values.of(endIndex));
  }
}
