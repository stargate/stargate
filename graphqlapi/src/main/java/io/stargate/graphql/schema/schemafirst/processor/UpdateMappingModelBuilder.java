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
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;

class UpdateMappingModelBuilder extends MutationMappingModelBuilder {

  private final FieldDefinition mutation;
  private final String parentTypeName;
  private final Map<String, EntityMappingModel> entities;

  UpdateMappingModelBuilder(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      Map<String, ResponseMappingModel> responses,
      ProcessingContext context) {
    super(context, mutation.getSourceLocation());
    this.mutation = mutation;
    this.parentTypeName = parentTypeName;
    this.entities = entities;
  }

  @Override
  MutationMappingModel build() throws SkipException {

    // TODO more options for signature
    // Currently requiring exactly one argument that must be an entity input with all PK fields set.
    // We could also take the PK fields directly (need a way to specify the entity), partial PKs for
    // multi-row deletions, additional IF conditions, etc.

    Type<?> returnType = TypeHelper.unwrapNonNull(mutation.getType());
    if (!(returnType instanceof TypeName) || !"Boolean".equals(((TypeName) returnType).getName())) {
      context.addError(
          returnType.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: updates can only return Boolean",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    List<InputValueDefinition> inputs = mutation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: updates must take the entity input type as the first argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    if (inputs.size() > 1) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: updates can't have more than one argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    InputValueDefinition input = inputs.get(0);
    EntityMappingModel entity = findEntity(input, entities, context, mutation.getName(), "update");
    return new UpdateMappingModel(parentTypeName, mutation, entity, input.getName());
  }
}
