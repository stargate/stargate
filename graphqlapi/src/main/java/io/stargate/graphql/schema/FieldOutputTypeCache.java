package io.stargate.graphql.schema;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.types.GqlMapBuilder;
import java.util.List;

/**
 * Caches GraphQL field output types, for example 'String' in:
 *
 * <pre>
 * type Books {
 *   author: String
 *   title: String
 * }
 * </pre>
 *
 * These are used when returning data, for example in response to a 'books' query.
 */
class FieldOutputTypeCache extends FieldTypeCache<GraphQLOutputType> {

  private final List<String> warnings;

  FieldOutputTypeCache(NameMapping nameMapping, List<String> warnings) {
    super(nameMapping);
    this.warnings = warnings;
  }

  @Override
  protected GraphQLOutputType compute(Column.ColumnType columnType) {
    if (columnType.isMap()) {
      GraphQLType keyType = get(columnType.parameters().get(0));
      GraphQLType valueType = get(columnType.parameters().get(1));
      return ((GraphQLOutputType) new GqlMapBuilder(keyType, valueType, false).build());
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

  private GraphQLOutputType computeUdt(UserDefinedType udt) {
    String graphqlName = nameMapping.getGraphqlName(udt);
    if (graphqlName == null) {
      throw new IllegalArgumentException(
          String.format(
              "Could find a GraphQL name mapping for UDT %s, "
                  + "this is probably because it clashes with another UDT",
              udt.name()));
    }
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name(graphqlName);
    for (Column column : udt.columns()) {
      String graphqlFieldName = nameMapping.getGraphqlName(udt, column);
      if (graphqlFieldName != null) {
        try {
          builder.field(
              new GraphQLFieldDefinition.Builder()
                  .name(graphqlFieldName)
                  .type(get(column.type()))
                  .build());
        } catch (Exception e) {
          warnings.add(
              String.format(
                  "Could not create output type for UDT field %s.%s, skipping (%s)",
                  column.table(), column.name(), e.getMessage()));
        }
      }
    }
    return builder.build();
  }
}
