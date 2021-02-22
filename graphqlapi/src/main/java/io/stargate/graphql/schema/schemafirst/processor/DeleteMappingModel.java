package io.stargate.graphql.schema.schemafirst.processor;

import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.DeleteFetcher;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.List;
import java.util.Map;

public class DeleteMappingModel extends MutationMappingModel {

  private final EntityMappingModel entity;
  private final String entityArgumentName;

  private DeleteMappingModel(
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
    return new DeleteFetcher(
        this, mappingModel, authenticationService, authorizationService, dataStoreFactory);
  }

  public static MutationMappingModel build(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context)
      throws SkipException {

    // TODO more options for signature
    // Currently requiring exactly one argument that must be an entity input with all PK fields set.
    // We could also take the PK fields directly (need a way to specify the entity), partial PKs for
    // multi-row deletions, additional arguments like ifExists, etc.

    Type<?> returnType = TypeHelper.unwrapNonNull(mutation.getType());
    if (!(returnType instanceof TypeName) || !"Boolean".equals(((TypeName) returnType).getName())) {
      context.addError(
          returnType.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: deletes can only return Boolean",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    List<InputValueDefinition> inputs = mutation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: deletes must take the entity input type as the first argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    if (inputs.size() > 1) {
      context.addError(
          mutation.getSourceLocation(),
          ProcessingErrorType.InvalidMapping,
          "Mutation %s: deletes can't have more than one argument",
          mutation.getName());
      throw SkipException.INSTANCE;
    }

    InputValueDefinition input = inputs.get(0);
    EntityMappingModel entity = findEntity(input, entities, context, mutation.getName(), "delete");
    return new DeleteMappingModel(parentTypeName, mutation, entity, input.getName());
  }
}
