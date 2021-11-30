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

import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class DeleteQueryBuilder {

  public DeleteQueryBuilder() {}

  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table) {
    QueryBuilder.QueryBuilder__40 builder =
        queryBuilder.get().delete().from(keyspace, table).timestamp().where(getPredicates());

    // then all bind able predicates
    for (Map.Entry<String, Predicate> entry : getBindPredicates().entrySet()) {
      builder = builder.where(entry.getKey(), entry.getValue());
    }

    return builder.build();
  }

  /** @return All fixed predicates. */
  protected Collection<BuiltCondition> getPredicates() {
    return Collections.emptyList();
  }

  /** @return Predicates that depends on the binding value. */
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.singletonMap(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ);
  }
}
