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
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** A GraphQL operation that will be translated into a CQL query. */
public abstract class OperationModel {

  private final FieldCoordinates coordinates;

  protected OperationModel(String parentTypeName, FieldDefinition field) {
    this.coordinates = FieldCoordinates.coordinates(parentTypeName, field.getName());
  }

  public FieldCoordinates getCoordinates() {
    return coordinates;
  }

  public abstract DataFetcher<?> getDataFetcher(MappingModel mappingModel);

  public interface ReturnType {
    Optional<EntityModel> getEntity();

    boolean isList();
  }

  public abstract static class EntityReturnTypeBase implements ReturnType {
    private final EntityModel entity;

    protected EntityReturnTypeBase(EntityModel entity) {
      this.entity = entity;
    }

    @Override
    public Optional<EntityModel> getEntity() {
      return Optional.of(entity);
    }
  }

  public static class EntityReturnType extends EntityReturnTypeBase {
    protected EntityReturnType(EntityModel entityName) {
      super(entityName);
    }

    @Override
    public boolean isList() {
      return false;
    }
  }

  public static class EntityListReturnType extends EntityReturnTypeBase {
    protected EntityListReturnType(EntityModel entityName) {
      super(entityName);
    }

    @Override
    public boolean isList() {
      return true;
    }
  }

  public static class ResponsePayloadModelListReturnType extends EntityReturnTypeBase {
    private final ResponsePayloadModel responsePayloadModel;

    protected ResponsePayloadModelListReturnType(ResponsePayloadModel responsePayloadModel) {
      super(responsePayloadModel.getEntity().orElse(null));
      this.responsePayloadModel = responsePayloadModel;
    }

    public ResponsePayloadModel getResponsePayloadModel() {
      return responsePayloadModel;
    }

    @Override
    public boolean isList() {
      return true;
    }
  }

  public static class SimpleListReturnType implements ReturnType {
    private final SimpleReturnType simpleReturnType;

    public SimpleListReturnType(SimpleReturnType simpleReturnType) {
      this.simpleReturnType = simpleReturnType;
    }

    @Override
    public Optional<EntityModel> getEntity() {
      return Optional.empty();
    }

    @Override
    public boolean isList() {
      return true;
    }

    public SimpleReturnType getSimpleReturnType() {
      return simpleReturnType;
    }
  }

  public enum SimpleReturnType implements ReturnType {
    BOOLEAN(Scalars.GraphQLBoolean.getName()),
    ;

    private static final Map<String, SimpleReturnType> FROM_TYPE_NAME =
        Arrays.stream(values()).collect(Collectors.toMap(v -> v.typeName, Function.identity()));

    private final String typeName;

    SimpleReturnType(String typeName) {
      this.typeName = typeName;
    }

    @Override
    public Optional<EntityModel> getEntity() {
      return Optional.empty();
    }

    @Override
    public boolean isList() {
      return false;
    }

    public static SimpleReturnType fromTypeName(String typeName) {
      return FROM_TYPE_NAME.get(typeName);
    }
  }
}
