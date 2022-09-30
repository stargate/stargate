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

package io.stargate.web.docsapi.service.write.db;

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.List;
import java.util.function.Consumer;

/** Deletes any array elements in the given document sub-path. */
public class DeleteSubDocumentArrayQueryBuilder extends DeleteSubDocumentPathQueryBuilder {

  /**
   * Constructs the query builder for deleting sub-document rows that represent array values.
   *
   * @param subDocumentPath The sub-document path that contains the elements
   * @param maxDepth Max depth of the document table
   */
  public DeleteSubDocumentArrayQueryBuilder(List<String> subDocumentPath, int maxDepth) {
    super(subDocumentPath, false, maxDepth);
  }

  /** {@inheritDoc} */
  @Override
  protected Consumer<QueryBuilder.QueryBuilder__40> whereConsumer() {
    int subPathSize = subDocumentPath.size();
    if (subPathSize + 1 > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    Consumer<QueryBuilder.QueryBuilder__40> superConsumer = super.whereConsumer();
    return where -> {
      superConsumer.accept(where);

      // fixed values for all queries, no need to bind
      String targetColumns = DocsApiConstants.P_COLUMN_NAME.apply(subPathSize);
      where.where(targetColumns, Predicate.GTE, "[000000]");
      where.where(targetColumns, Predicate.LTE, "[999999]");
    };
  }
}
