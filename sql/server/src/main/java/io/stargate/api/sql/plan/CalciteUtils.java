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
package io.stargate.api.sql.plan;

import io.stargate.api.sql.plan.exec.RuntimeContext;
import io.stargate.db.datastore.DataStore;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;

public class CalciteUtils {

  public static final String DS_CONTEXT_BUILDER_KEY = "__ds_context_builder";

  public static RuntimeContext.Builder contextBuilder(EnumerableRelImplementor implementor) {
    return (RuntimeContext.Builder) implementor.map.get(DS_CONTEXT_BUILDER_KEY);
  }

  public static RuntimeContext.Builder initBuilder(
      DataStore dataStore, Map<String, Object> parameters) {
    RuntimeContext.Builder builder = new RuntimeContext.Builder(dataStore);
    parameters.put(DS_CONTEXT_BUILDER_KEY, builder);
    return builder;
  }
}
