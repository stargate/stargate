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
 * Delete query builder that targets a sub-path of a document.
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
 * The subpath ["b","c"] with exact=true will generate the DELETE conditions:
 *
 * <pre>
 * WHERE key = 'xyz'
 *   AND p0 = 'b'
 *   AND p1 = 'c'
 *   AND p2 = ''
 * </pre>
 *
 * The subpath ["b"] with exact=false will generate the DELETE conditions:
 *
 * <pre>
 * WHERE key = 'xyz'
 *   AND p0 = 'b'
 * </pre>
 */
public class DeleteSubDocumentPathQueryBuilder extends AbstractDeleteQueryBuilder {

  protected final List<String> subDocumentPath;
  protected final boolean exactPath;
  protected final int maxDepth;

  /**
   * Constructs the query builder for deleting sub-document rows for a single document.
   *
   * @param subDocumentPath The sub-document path to delete
   * @param exactPath If <code>true</code> deletes only the exact sub-path and leaves any deeper
   *     nested paths untouched.
   */
  public DeleteSubDocumentPathQueryBuilder(
      List<String> subDocumentPath, boolean exactPath, DocumentProperties documentProperties) {
    super(documentProperties);
    this.subDocumentPath = subDocumentPath;
    this.exactPath = exactPath;
    this.maxDepth = documentProperties.maxDepth();
  }

  @Override
  protected void addWhereConditions(List<BuiltCondition> whereConditions) {
    if (subDocumentPath.size() > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    int pathSize = exactPath ? maxDepth : subDocumentPath.size();
    for (int i = 0; i < pathSize; i++) {
      whereConditions.add(
          BuiltCondition.of(
              documentProperties.tableProperties().pathColumnName(i), Predicate.EQ, Term.marker()));
    }
  }

  @Override
  protected void addBindValues(List<Value> values) {
    int pathSize = exactPath ? maxDepth : subDocumentPath.size();
    for (int i = 0; i < pathSize; i++) {
      if (i < subDocumentPath.size()) {
        values.add(Values.of(subDocumentPath.get(i)));
      } else {
        values.add(Values.of(""));
      }
    }
  }
}
