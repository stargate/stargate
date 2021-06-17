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

import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.Parameters;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MutationModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** An INSERT, UPDATE or DELETE mutation. */
public abstract class MutationFetcher<MutationModelT extends MutationModel, ResultT>
    extends DeployedFetcher<ResultT> {

  protected final MutationModelT model;

  protected MutationFetcher(MutationModelT model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  @Override
  protected Parameters getDatastoreParameters(DataFetchingEnvironment environment) {
    ConsistencyLevel consistencyLevel = model.getConsistencyLevel().orElse(DEFAULT_CONSISTENCY);
    ConsistencyLevel serialConsistencyLevel =
        model.getSerialConsistencyLevel().orElse(DEFAULT_SERIAL_CONSISTENCY);
    if (consistencyLevel == DEFAULT_CONSISTENCY
        && serialConsistencyLevel == DEFAULT_SERIAL_CONSISTENCY) {
      return DEFAULT_PARAMETERS;
    } else {
      return DEFAULT_PARAMETERS
          .toBuilder()
          .consistencyLevel(consistencyLevel)
          .serialConsistencyLevel(serialConsistencyLevel)
          .build();
    }
  }

  protected Collection<ValueModifier> buildModifiers(
      EntityModel entityModel,
      Keyspace keyspace,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument) {

    List<ValueModifier> modifiers = new ArrayList<>();
    for (FieldModel column : entityModel.getRegularColumns()) {
      String graphqlName = column.getGraphqlName();

      if (hasArgument.test(graphqlName)) {
        Object graphqlValue = getArgument.apply(graphqlName);
        modifiers.add(
            ValueModifier.set(
                column.getCqlName(), toCqlValue(graphqlValue, column.getCqlType(), keyspace)));
      }
    }
    if (modifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Input object must have at least one non-PK field set for an update");
    }
    return modifiers;
  }
}
