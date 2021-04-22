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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * An entity returned by {@link FederatedEntityFetcher}.
 *
 * <p>This is a thin wrapper around another map, we just need to add {@link #getTypeName()} to allow
 * GraphQL to resolve the concrete type.
 */
public class FederatedEntity extends AbstractMap<String, Object> {

  private final String typeName;
  private final Map<String, Object> delegate;

  private FederatedEntity(String typeName, Map<String, Object> delegate) {
    this.typeName = typeName;
    this.delegate = delegate;
  }

  public String getTypeName() {
    return typeName;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public Object get(Object key) {
    // We don't *have* to override this, but we can do better than the default implementation:
    return delegate.get(key);
  }

  public static FederatedEntity wrap(EntityModel mappingModel, Map<String, Object> rawData) {
    return rawData == null ? null : new FederatedEntity(mappingModel.getGraphqlName(), rawData);
  }
}
