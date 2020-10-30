package io.stargate.graphql.schema;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.types.GqlMapBuilder;
import java.util.List;

/**
 * Caches GraphQL field input types, for example 'String' in:
 *
 * <pre>
 * input BooksInput {
 *   author: String
 *   title: String
 * }
 * </pre>
 *
 * These are used when receiving data, for in an 'insertBooks' mutation.
 */
class FieldInputTypeCache extends FieldTypeCache<GraphQLInputType> {

  private final List<String> warnings;

  FieldInputTypeCache(NameMapping nameMapping, List<String> warnings) {
    super(nameMapping);
    this.warnings = warnings;
  }

  @Override
  protected GraphQLInputType compute(Column.ColumnType columnType) {
    if (columnType.isMap()) {
      GraphQLType keyType = get(columnType.parameters().get(0));
      GraphQLType valueType = get(columnType.parameters().get(1));
      return ((GraphQLInputType) new GqlMapBuilder(keyType, valueType, true).build());
    } else if (columnType.isList() || columnType.isSet()) {
      return new GraphQLList(get(columnType.parameters().get(0)));
    } else if (columnType.isUserDefined()) {
      UserDefinedType udt = (UserDefinedType) columnType;
      return computeUdt(udt);
    } else if (columnType.isTuple()) {
      throw new UnsupportedOperationException("Tuples are not implemented yet");
    } else {
      return getScalar(columnType.rawType());
    }
  }

  private GraphQLInputType computeUdt(UserDefinedType udt) {
    String graphqlName = nameMapping.getGraphqlName(udt);
    if (graphqlName == null) {
      throw new IllegalArgumentException(
          String.format(
              "Could find a GraphQL name mapping for UDT %s, "
                  + "this is probably because it clashes with another UDT",
              udt.name()));
    }
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject().name(graphqlName + "Input");
    for (Column column : udt.columns()) {
      String graphqlFieldName = nameMapping.getGraphqlName(udt, column);
      if (graphqlFieldName != null) {
        try {
          builder.field(
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlFieldName)
                  .type(get(column.type()))
                  .build());
        } catch (Exception e) {
          warnings.add(
              String.format(
                  "Could not create input type for field %s in UDT %s, skipping (%s)",
                  column.name(), column.table(), e.getMessage()));
        }
      }
    }
    return builder.build();
  }
}
