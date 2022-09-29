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

/** Delete query builder that targets a sub-path of a document. */
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
   * @param maxDepth Max depth of the document table
   */
  public DeleteSubDocumentPathQueryBuilder(
      List<String> subDocumentPath, boolean exactPath, int maxDepth) {
    this.subDocumentPath = subDocumentPath;
    this.exactPath = exactPath;
    this.maxDepth = maxDepth;
  }

  /** {@inheritDoc} */
  @Override
  protected Consumer<QueryBuilder.QueryBuilder__40> whereConsumer() {
    if (subDocumentPath.size() > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    return where -> {
      int pathSize = exactPath ? maxDepth : subDocumentPath.size();
      // if we have the sub-document path add that as well
      for (int i = 0; i < pathSize; i++) {
        where.where(DocsApiConstants.P_COLUMN_NAME.apply(i), Predicate.EQ);
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  protected Object[] getBindValues(String documentId, long timestamp) {
    Object[] baseValues = super.getBindValues(documentId, timestamp);

    int baseValuesSize = baseValues.length;
    int pathSize = exactPath ? maxDepth : subDocumentPath.size();

    // base (timestamp and document id) as first
    Object[] values = new Object[baseValuesSize + pathSize];
    System.arraycopy(baseValues, 0, values, 0, baseValuesSize);

    // then sub-document paths based on the
    for (int i = 0; i < pathSize; i++) {
      if (i < subDocumentPath.size()) {
        values[i + baseValuesSize] = subDocumentPath.get(i);
      } else {
        values[i + baseValuesSize] = "";
      }
    }

    return values;
  }
}
