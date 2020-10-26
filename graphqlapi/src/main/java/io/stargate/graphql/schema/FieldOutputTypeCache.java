package io.stargate.graphql.schema;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.types.GqlMapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(FieldOutputTypeCache.class);

  FieldOutputTypeCache(NameMapping nameMapping) {
    super(nameMapping);
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
    GraphQLObjectType.Builder builder =
        GraphQLObjectType.newObject().name(nameMapping.getGraphqlName(udt));
    for (Column column : udt.columns()) {
      try {
        builder.field(
            new GraphQLFieldDefinition.Builder()
                .name(nameMapping.getGraphqlName(udt, column))
                .type(get(column.type()))
                .build());
      } catch (Exception e) {
        // TODO find a better way to surface errors
        LOG.error(String.format("Type for %s could not be created", column.name()), e);
      }
    }
    return builder.build();
  }
}
