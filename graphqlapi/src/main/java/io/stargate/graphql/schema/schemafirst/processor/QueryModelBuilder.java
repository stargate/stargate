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

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import java.util.List;
import java.util.Map;

class QueryModelBuilder extends ModelBuilderBase<QueryModel> {

  private final FieldDefinition query;
  private final String parentTypeName;
  private final Map<String, EntityModel> entities;

  QueryModelBuilder(
      FieldDefinition query,
      String parentTypeName,
      Map<String, EntityModel> entities,
      Map<String, ResponseModel> responses,
      ProcessingContext context) {
    super(context, query.getSourceLocation());
    this.query = query;
    this.parentTypeName = parentTypeName;
    this.entities = entities;
  }

  QueryModel build() throws SkipException {
    Type<?> returnType = query.getType();
    boolean isListType = returnType instanceof ListType;
    String entityName = null;
    if ((returnType instanceof TypeName)) {
      entityName = ((TypeName) returnType).getName();
    } else if (isListType) {
      ListType listType = (ListType) returnType;
      if (listType.getType() instanceof TypeName) {
        entityName = ((TypeName) listType.getType()).getName();
      }
    }
    if (entityName == null || !entities.containsKey(entityName)) {
      invalidMapping(
          "Query %s: expected the return type to be an object (or list of objects) that maps to "
              + "an entity",
          query.getName());
      throw SkipException.INSTANCE;
    }

    EntityModel entity = entities.get(entityName);

    List<InputValueDefinition> inputValues = query.getInputValueDefinitions();
    List<FieldModel> partitionKey = entity.getPartitionKey();
    if (inputValues.size() < partitionKey.size()) {
      invalidMapping(
          "Query %s: expected to have at least enough arguments to cover the partition key "
              + "(%d needed, %d provided).",
          query.getName(), partitionKey.size(), inputValues.size());
      throw SkipException.INSTANCE;
    }
    List<FieldModel> primaryKey = entity.getPrimaryKey();

    boolean foundErrors = false;
    ImmutableList.Builder<String> inputNames = ImmutableList.builder();
    for (int i = 0; i < inputValues.size(); i++) {
      InputValueDefinition argument = inputValues.get(i);
      FieldModel field = primaryKey.get(i);

      Type<?> argumentType = argument.getType();
      if (!argumentType.isEqualTo(field.getGraphqlType())) {
        invalidMapping(
            "Query %s: expected argument %s to have the same type as %s.%s",
            query.getName(), argument.getName(), entity.getGraphqlName(), field.getGraphqlName());
        foundErrors = true;
      }

      inputNames.add(argument.getName());
    }

    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return new QueryModel(parentTypeName, query, entity, inputNames.build(), isListType);
  }
}
