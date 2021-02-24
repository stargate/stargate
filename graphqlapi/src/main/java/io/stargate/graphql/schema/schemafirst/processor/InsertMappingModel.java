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
package io.stargate.graphql.schema.schemafirst.processor;

import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.InsertFetcher;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;

public class InsertMappingModel extends MutationMappingModel {

  private final EntityMappingModel entity;
  private final String entityArgumentName;

  private InsertMappingModel(
      String parentTypeName,
      FieldDefinition field,
      EntityMappingModel entity,
      String entityArgumentName) {
    super(parentTypeName, field);
    this.entity = entity;
    this.entityArgumentName = entityArgumentName;
  }

  public EntityMappingModel getEntity() {
    return entity;
  }

  public String getEntityArgumentName() {
    return entityArgumentName;
  }

  @Override
  public DataFetcher<?> getDataFetcher(
      MappingModel mappingModel,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    return new InsertFetcher(
        this, mappingModel, authenticationService, authorizationService, dataStoreFactory);
  }

  public static MutationMappingModel build(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context)
      throws SkipException {

    List<InputValueDefinition> inputs = mutation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: inserts must take the entity input type as the first argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    // TODO we'll probably allow more parameters in the future, e.g. ifNotExists, etc.
    if (inputs.size() > 1) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: inserts can't have more than one argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    InputValueDefinition input = inputs.get(0);
    EntityMappingModel entity = findEntity(input, entities, context, mutation.getName(), "insert");
    if (!matchesReturnType(entity, mutation, context)) {
      throw SkipException.INSTANCE;
    }
    return new InsertMappingModel(parentTypeName, mutation, entity, input.getName());
  }

  private static boolean matchesReturnType(
      EntityMappingModel entity, FieldDefinition mutation, ProcessingContext context) {
    // TODO allow other return types
    // Something simpler if there are no generated fields, or a more complex type if there is more
    // info (eg for LWTs).
    Type<?> returnType = TypeHelper.unwrapNonNull(mutation.getType());

    boolean matches =
        (returnType instanceof TypeName)
            && ((TypeName) returnType).getName().equals(entity.getGraphqlName());
    if (!matches) {
      context.addError(
          returnType.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: unexpected return type for inserts",
          mutation.getName());
    }
    return matches;
  }
}
