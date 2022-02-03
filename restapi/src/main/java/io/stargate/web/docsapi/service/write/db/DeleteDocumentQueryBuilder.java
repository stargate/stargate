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

import io.stargate.db.query.builder.QueryBuilder;
import java.util.function.Consumer;

/** Delete query builder that deletes a complete document. */
public class DeleteDocumentQueryBuilder extends AbstractDeleteQueryBuilder {

  // static instance to avoid reinitialization
  public static DeleteDocumentQueryBuilder INSTANCE = new DeleteDocumentQueryBuilder();

  /** {@inheritDoc} */
  @Override
  protected Consumer<QueryBuilder.QueryBuilder__40> whereConsumer() {
    return where -> {};
  }
}
