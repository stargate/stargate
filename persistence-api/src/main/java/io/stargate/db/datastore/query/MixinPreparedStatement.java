/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.datastore.query;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Mixes in prepared parameters to those supplied at execution. */
class MixinPreparedStatement implements PreparedStatement {
  private final long unboundParameters;
  private final PreparedStatement prepared;
  private final List<Parameter<?>> parameters;

  public MixinPreparedStatement(PreparedStatement prepared, List<Parameter<?>> parameters) {
    Preconditions.checkNotNull(prepared);
    this.prepared = prepared;
    this.parameters = parameters;
    unboundParameters = parameters.stream().filter(p -> !p.value().isPresent()).count();
    boolean bindingFunctionsFound =
        parameters.stream().anyMatch(p -> p.bindingFunction().isPresent());
    boolean arrayParametersFound =
        parameters.stream()
            .anyMatch(p -> !p.value().isPresent() && !p.bindingFunction().isPresent());
    Preconditions.checkArgument(
        bindingFunctionsFound ^ arrayParametersFound
            || !bindingFunctionsFound && !arrayParametersFound,
        "Mixin prepared statement may not have dynamic bindings and array bindings");
  }

  @Override
  public CompletableFuture<ResultSet> execute(
      Optional<ConsistencyLevel> consistencyLevel, Object... parameters) {
    Preconditions.checkArgument(
        parameters.length == unboundParameters,
        "Unexpected number of arguments. Expected %s but got %s. Statement: %s.",
        unboundParameters,
        parameters.length,
        prepared);
    List<Object> mergedParameters = new ArrayList<>(this.parameters.size());
    int mergeCount = 0;
    for (Parameter<?> parameter : this.parameters) {
      if (parameter.ignored()) {
        if (!parameter.value().isPresent()) {
          mergeCount++;
        }
        continue;
      }
      if (parameter.value().isPresent()) {
        mergedParameters.add(parameter.value().get());
      } else {
        mergedParameters.add(parameters[mergeCount++]);
      }
    }
    return prepared.execute(consistencyLevel, mergedParameters.toArray());
  }

  @Override
  public String toString() {
    return prepared.toString();
  }
}
