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

package io.stargate.web.docsapi.service.query.search.resolver;

import io.reactivex.rxjava3.core.Flowable;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;

/** Base interface for all the document resolvers. */
public interface DocumentsResolver {

  /**
   * Returns documents.
   *
   * @param queryExecutor
   * @param keyspace keyspace to search in
   * @param collection collection to search in
   * @param paginator {@link Paginator}
   * @return Flowable of documents.
   */
  Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator);
}
