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
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

public abstract class MutationMappingModel extends OperationMappingModel {

  public MutationMappingModel(String parentTypeName, FieldDefinition field) {
    super(parentTypeName, field);
  }

  protected static EntityMappingModel findEntity(
      InputValueDefinition input,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context,
      String mutationName,
      String mutationKind)
      throws SkipException {

    Type<?> type = TypeHelper.unwrapNonNull(input.getType());

    if (type instanceof ListType) {
      context.addError(
          input.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: unexpected list type, %ss expect a single entity",
          mutationName,
          mutationKind);
      throw SkipException.INSTANCE;
    }

    String inputTypeName = ((TypeName) type).getName();
    Optional<EntityMappingModel> entity =
        entities.values().stream()
            .filter(e -> e.getInputTypeName().map(name -> name.equals(inputTypeName)).orElse(false))
            .findFirst();
    if (!entity.isPresent()) {
      context.addError(
          input.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: unexpected type, %ss expect an input object that maps to a CQL entity",
          mutationName,
          mutationKind);
      throw SkipException.INSTANCE;
    }
    return entity.get();
  }
}
