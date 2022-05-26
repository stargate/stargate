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
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import java.util.List;

/**
 * Deletes given keys in the given document sub-path.
 *
 * <p>For example, given the document:
 *
 * <pre>
 * { "a": 1, "b": { "c": 2, "d": 3} }
 * </pre>
 *
 * That gets stored as the following rows:
 *
 * <pre>
 * key | p0 | p1 | p2 | dbl_value
 * --- +----+----+----+-----------
 * xyz |  a |    |    |         1
 * xyz |  b |  c |    |         2
 * xyz |  b |  d |    |         3
 * </pre>
 *
 * The subpath ["b"] with keys ["d", "e"] will generate the DELETE conditions:
 *
 * <pre>
 * WHERE key = 'xyz'
 *   AND p0 = 'b'
 *   AND p1 IN ('d', 'e')
 * </pre>
 */
public class DeleteSubDocumentKeysQueryBuilder extends DeleteSubDocumentPathQueryBuilder {

  private final List<String> keys;

  /**
   * Constructs the query builder for deleting sub-document rows that represent a key.
   *
   * @param subDocumentPath The sub-document path that contains the elements
   * @param keys Keys to delete at the given path. Must not be empty.
   */
  public DeleteSubDocumentKeysQueryBuilder(
      List<String> subDocumentPath, List<String> keys, DocumentProperties documentProperties) {
    super(subDocumentPath, false, documentProperties);
    this.keys = keys;
  }

  @Override
  protected void addWhereConditions(List<BuiltCondition> whereConditions) {
    super.addWhereConditions(whereConditions);

    int subPathSize = subDocumentPath.size();
    if (subPathSize + 1 > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    String targetColumns = documentProperties.tableProperties().pathColumnName(subPathSize);
    whereConditions.add(BuiltCondition.of(targetColumns, Predicate.IN, Term.marker()));
  }

  @Override
  protected void addBindValues(List<Value> values) {
    super.addBindValues(values);

    values.add(Values.of(keys.stream().map(Values::of).toList()));
  }
}
