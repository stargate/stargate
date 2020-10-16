package io.stargate.graphql.schema;

import static graphql.schema.GraphQLList.list;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLScalarType;
import io.stargate.db.schema.Column;

/**
 * Caches GraphQL field "filter input" types, for example 'StringFilterInput' in:
 *
 * <pre>
 * input BooksFilterInput {
 *   author: StringFilterInput
 *   title: StringFilterInput
 * }
 * </pre>
 *
 * These are used when receiving filtering criteria, for example in ra 'books' query:
 *
 * <pre>
 * query {
 *   books(filter: { author: { eq: "Cormac McCarthy" } }) { ... }
 * }
 * </pre>
 */
public class FieldFilterInputTypeCache extends FieldTypeCache<GraphQLInputType> {

  private final FieldInputTypeCache inputTypeCache;

  FieldFilterInputTypeCache(FieldInputTypeCache inputTypeCache, NameMapping nameMapping) {
    super(nameMapping);
    this.inputTypeCache = inputTypeCache;
  }

  @Override
  protected GraphQLInputType compute(Column.ColumnType columnType) {
    GraphQLInputType gqlInputType = inputTypeCache.get(columnType);
    if (columnType.isCollection()) {
      return collectionFilter(gqlInputType);
    } else {
      return singleElementFilter(gqlInputType);
    }
  }

  private GraphQLInputType collectionFilter(GraphQLInputType gqlInputType) {
    throw new UnsupportedOperationException("TODO implement collection filters");
  }

  private static GraphQLInputObjectType singleElementFilter(GraphQLInputType gqlInputType) {
    return GraphQLInputObjectType.newInputObject()
        .name(getName(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("eq").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("notEq").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gt").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gte").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lt").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lte").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("in").type(list(gqlInputType)))
        .build();
  }

  private static String getName(GraphQLInputType gqlInputType) {
    if (gqlInputType instanceof GraphQLScalarType) {
      return ((GraphQLScalarType) gqlInputType).getName() + "FilterInput";
    } else if (gqlInputType instanceof GraphQLInputObjectType) {
      return ((GraphQLInputObjectType) gqlInputType).getName().replace("Input", "FilterInput");
    } else {
      throw new IllegalStateException("Unexpected input type " + gqlInputType);
    }
  }
}
