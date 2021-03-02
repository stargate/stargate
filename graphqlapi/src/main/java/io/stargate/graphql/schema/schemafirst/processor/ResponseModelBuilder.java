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
import graphql.language.ListType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import io.stargate.graphql.schema.schemafirst.processor.EntityModel.Target;
import io.stargate.graphql.schema.schemafirst.processor.ResponseModel.EntityField;
import io.stargate.graphql.schema.schemafirst.processor.ResponseModel.TechnicalField;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.EnumSet;
import java.util.Optional;

class ResponseModelBuilder extends ModelBuilderBase<ResponseModel> {

  private final ObjectTypeDefinition type;
  private final String graphqlName;

  ResponseModelBuilder(ObjectTypeDefinition type, ProcessingContext context) {
    super(context, type.getSourceLocation());
    this.type = type;
    this.graphqlName = type.getName();
  }

  @Override
  ResponseModel build() {
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
    return new ResponseModel(Optional.ofNullable(entityField), technicalFields);
  }

  private EntityField asEntityField(FieldDefinition field) {
    Type<?> type = TypeHelper.unwrapNonNull(field.getType());
    boolean isList = type instanceof ListType;
    if (isList) {
      type = TypeHelper.unwrapNonNull(((ListType) type).getType());
    }
    assert type instanceof TypeName;
    String typeName = ((TypeName) type).getName();
    return context
        .getTypeRegistry()
        .getType(typeName)
        .filter(this::isTableEntity)
        .map(
            entityDefinition ->
                new EntityField(field.getName(), entityDefinition.getName(), isList))
        .orElse(null);
  }

  private boolean isTableEntity(TypeDefinition<?> typeDefinition) {
    if (!(typeDefinition instanceof ObjectTypeDefinition)
        || DirectiveHelper.getDirective("cql_payload", typeDefinition).isPresent()) {
      return false;
    }
    Target target =
        DirectiveHelper.getDirective("cql_entity", typeDefinition)
            .flatMap(d -> DirectiveHelper.getEnumArgument(d, "target", Target.class, context))
            .orElse(Target.TABLE);
    return target == Target.TABLE;
  }
}
