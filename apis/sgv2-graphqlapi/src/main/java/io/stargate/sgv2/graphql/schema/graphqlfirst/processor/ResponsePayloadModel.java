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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import graphql.Scalars;
import graphql.language.FieldDefinition;
import graphql.language.TypeName;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.EnumSet;
import java.util.Optional;

/**
 * Represents a GraphQL object type that has been annotated with the {@code cql_payload} directive.
 * This is a transient object (not mapped to a CQL table), that acts as a wrapper for operation
 * responses.
 */
public class ResponsePayloadModel implements OperationModel.ReturnType {

  private final Optional<EntityField> entityField;
  private final EnumSet<TechnicalField> technicalFields;

  public ResponsePayloadModel(
      Optional<EntityField> entityField, EnumSet<TechnicalField> technicalFields) {
    this.entityField = entityField;
    this.technicalFields = technicalFields;
  }

  /**
   * The field in the response object that references a mapped entity (or list thereof). There is at
   * most one such field.
   */
  public Optional<EntityField> getEntityField() {
    return entityField;
  }

  @Override
  public Optional<EntityModel> getEntity() {
    return getEntityField().map(EntityField::getEntity);
  }

  @Override
  public boolean isList() {
    return getEntityField().map(EntityField::isList).orElse(false);
  }

  /**
   * An set of additional metadata fields that Stargate will know how to fill automatically. They
   * have predefined names and types.
   */
  public EnumSet<TechnicalField> getTechnicalFields() {
    return technicalFields;
  }

  public static class EntityField {

    private final String name;
    private final EntityModel entity;
    private final boolean isList;

    public EntityField(String name, EntityModel entity, boolean isList) {
      this.name = name;
      this.entity = entity;
      this.isList = isList;
    }

    /** The name of the GraphQL field. */
    public String getName() {
      return name;
    }

    /** The entity that this field references. */
    public EntityModel getEntity() {
      return entity;
    }

    /** Whether the field is a list of entities, or a single entity. */
    public boolean isList() {
      return isList;
    }
  }

  public enum TechnicalField {
    APPLIED("applied", Scalars.GraphQLBoolean.getName()),
    PAGING_STATE("pagingState", Scalars.GraphQLString.getName()),
    ;

    private final String graphqlName;
    private final TypeName typeName;

    TechnicalField(String graphqlName, String typeName) {
      this.graphqlName = graphqlName;
      this.typeName = TypeName.newTypeName(typeName).build();
    }

    public String getGraphqlName() {
      return graphqlName;
    }

    static TechnicalField matching(FieldDefinition field) {
      for (TechnicalField value : values()) {
        if (value.matches(field)) {
          return value;
        }
      }
      return null;
    }

    private boolean matches(FieldDefinition field) {
      return graphqlName.equals(field.getName())
          && TypeHelper.deepEquals(typeName, TypeHelper.unwrapNonNull(field.getType()));
    }
  }
}
