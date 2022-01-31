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
import org.apache.commons.lang3.ArrayUtils;

/** Deletes given keys in the given document sub-path. */
public class DeleteSubDocumentKeysQueryBuilder extends DeleteSubDocumentPathQueryBuilder {

  private final List<String> keys;

  /**
   * Constructs the query builder for deleting sub-document rows that represent a key.
   *
   * @param subDocumentPath The sub-document path that contains the elements
   * @param keys Keys to delete at the given path. Must not be empty.
   * @param maxDepth Max depth of the document table
   */
  public DeleteSubDocumentKeysQueryBuilder(
      List<String> subDocumentPath, List<String> keys, int maxDepth) {
    super(subDocumentPath, false, maxDepth);

    if (keys == null || keys.isEmpty()) {
      throw new IllegalArgumentException("Keys to delete in the document path must not be empty.");
    }

    this.keys = keys;
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

      String targetColumns = DocsApiConstants.P_COLUMN_NAME.apply(subPathSize);
      where.where(targetColumns, Predicate.IN);
    };
  }

  @Override
  protected Object[] getBindValues(String documentId, long timestamp) {
    Object[] baseValues = super.getBindValues(documentId, timestamp);
    return ArrayUtils.add(baseValues, keys);
  }
}
