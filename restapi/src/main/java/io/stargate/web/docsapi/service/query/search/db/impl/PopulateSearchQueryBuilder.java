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

package io.stargate.web.docsapi.service.query.search.db.impl;

import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/** Simple query builder for document population. */
public class PopulateSearchQueryBuilder extends AbstractSearchQueryBuilder {

  @Override
  protected boolean allowFiltering() {
    return false;
  }

  @Override
  protected Collection<BuiltCondition> getPredicates() {
    return Collections.emptyList();
  }

  @Override
  protected Map<String, Predicate> getBindPredicates() {
    return Collections.singletonMap(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ);
  }
}
