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
package io.stargate.graphql.schema.graphqlfirst.processor;

import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.EntityField;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

class ResponsePayloadModelBuilder extends ModelBuilderBase<ResponsePayloadModel> {

  private final ObjectTypeDefinition type;
  private final String graphqlName;
  private final Map<String, EntityModel> entities;

  ResponsePayloadModelBuilder(
      ObjectTypeDefinition type, Map<String, EntityModel> entities, ProcessingContext context) {
    super(context, type.getSourceLocation());
    this.type = type;
    this.graphqlName = type.getName();
    this.entities = entities;
  }

  @Override
  ResponsePayloadModel build() {
    EntityField entityField = null;
    EnumSet<TechnicalField> technicalFields = EnumSet.noneOf(TechnicalField.class);

    for (FieldDefinition field : type.getFieldDefinitions()) {

      EntityField newEntityField = asEntityField(field);
      if (newEntityField != null) {
        if (entityField == null) {
          entityField = newEntityField;
        } else {
          invalidMapping(
              "%s: both %s and %s reference a mapped entity. There can only be one such field.",
              graphqlName, entityField.getName(), newEntityField.getName());
        }
        continue;
      }

      TechnicalField technicalField;
      if ((technicalField = TechnicalField.matching(field)) != null) {
        technicalFields.add(technicalField);
        continue;
      }

      invalidMapping(
          "%s: couldn't map field %s. It doesn't match any of the predefined technical fields, "
              + "nor does it reference a mapped entity.",
          graphqlName, field.getName());
    }
    return new ResponsePayloadModel(Optional.ofNullable(entityField), technicalFields);
  }

  private EntityField asEntityField(FieldDefinition field) {
    Type<?> type = TypeHelper.unwrapNonNull(field.getType());
    boolean isList = type instanceof ListType;
    if (isList) {
      type = TypeHelper.unwrapNonNull(((ListType) type).getType());
    }
    assert type instanceof TypeName;
    EntityModel entity = entities.get(((TypeName) type).getName());
    return entity == null ? null : new EntityField(field.getName(), entity, isList);
  }
}
