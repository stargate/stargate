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
import java.util.List;
import java.util.Map;
import java.util.Optional;

class InsertModelBuilder extends MutationModelBuilder {

  private final FieldDefinition mutation;
  private final String parentTypeName;
  private final Map<String, EntityModel> entities;
  private final Map<String, ResponseModel> responses;

  InsertModelBuilder(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityModel> entities,
      Map<String, ResponseModel> responses,
      ProcessingContext context) {
    super(context, mutation.getSourceLocation());
    this.mutation = mutation;
    this.parentTypeName = parentTypeName;
    this.entities = entities;
    this.responses = responses;
  }

  @Override
  InsertModel build() throws SkipException {

    boolean ifNotExists = computeIfNotExists();

    // Validate inputs: must be a single entity argument
    List<InputValueDefinition> inputs = mutation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      invalidMapping(
          "Mutation %s: inserts must take the entity input type as the first argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }
    if (inputs.size() > 1) {
      invalidMapping("Mutation %s: inserts can't have more than one argument", mutation.getName());
      throw SkipException.INSTANCE;
    }
    InputValueDefinition input = inputs.get(0);
    EntityModel entity = findEntity(input, entities, context, mutation.getName(), "insert");

    // Validate return type: must be the entity itself, or a wrapper payload
    Type<?> returnType = TypeHelper.unwrapNonNull(mutation.getType());
    if (returnType instanceof ListType) {
      invalidMapping("Mutation %s: unexpected list type", mutation.getName());
      throw SkipException.INSTANCE;
    }
    assert returnType instanceof TypeName;
    String returnTypeName = ((TypeName) returnType).getName();
    ResponseModel response = responses.get(returnTypeName);

    if (response != null) {
      if (response.getEntityField().isPresent()) {
        ResponseModel.EntityField entityField = response.getEntityField().get();
        if (entityField.isList() || returnTypeName.equals(entityField.getEntityName())) {
          invalidMapping(
              "Mutation %s: invalid return type. "
                  + "Expected a payload that wraps a single instance of %s.",
              mutation.getName(), entity.getGraphqlName());
          throw SkipException.INSTANCE;
        }
      }
    } else {
      if (!returnTypeName.equals(entity.getGraphqlName())) {
        invalidMapping(
            "Mutation %s: invalid return type. Expected the same entity as the argument (%s), "
                + "or a wrapper object.",
            mutation.getName(), entity.getGraphqlName());
        throw SkipException.INSTANCE;
      }
    }

    return new InsertModel(
        parentTypeName, mutation, entity, input.getName(), response, ifNotExists);
  }

  private Boolean computeIfNotExists() {
    // If the directive is set, it always takes precedence
    Optional<Boolean> fromDirective =
        DirectiveHelper.getDirective("cql_insert", mutation)
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "ifNotExists", context));
    if (fromDirective.isPresent()) {
      return fromDirective.get();
    }
    // Otherwise, try the naming convention
    if (mutation.getName().endsWith("IfNotExists")) {
      info(
          "Mutation %s: setting the 'ifNotExists' flag implicitly "
              + "because the name follows the naming convention.",
          mutation.getName());
      return true;
    }
    return false;
  }
}
