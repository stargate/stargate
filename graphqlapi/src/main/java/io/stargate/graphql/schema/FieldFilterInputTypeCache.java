package io.stargate.graphql.schema;

import static graphql.schema.GraphQLList.list;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
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
 * These are used when receiving filtering criteria, for example in a 'books' query:
 *
 * <pre>
 * query {
 *   books(filter: { author: { eq: "Cormac McCarthy" } }) { ... }
 * }
 * </pre>
 *
 * Note that these filters are also used in "DELETE IF" and "UPDATE IF" conditions. Some operators
 * only apply in one context, for example "notEq" is allowed in conditional updates, but not
 * selects. We currently have a single filter type with all the operators, it's up to the user to a
 * valid one otherwise the query will fail. We could possibly refine this by creating multiple
 * filter types depending on the context (whether the column is a collection, is a PK, has an SAI
 * index defined for it, etc).
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
    if (columnType.isMap()) {
      return mapFilter(gqlInputType);
    } else if (columnType.isList() || columnType.isSet()) {
      return listFilter(gqlInputType);
    } else {
      return singleElementFilter(gqlInputType);
    }
  }

  private GraphQLInputType mapFilter(GraphQLInputType gqlInputType) {

    // see GqlMapBuilder
    GraphQLList gqlMapType = (GraphQLList) gqlInputType;
    GraphQLInputObjectType entryType = (GraphQLInputObjectType) gqlMapType.getWrappedType();
    GraphQLNonNull nonNullkeyType = (GraphQLNonNull) entryType.getField("key").getType();
    GraphQLInputType keyType = (GraphQLInputType) nonNullkeyType.getWrappedType();
    GraphQLInputType valueType = entryType.getField("value").getType();

    // Basic operators are allowed for:
    // - conditional updates
    // - when the map column is the primary key
    GraphQLInputObjectType.Builder builder = basicComparisons(gqlInputType);

    // The 'contains*' operators are allowed for regular columns with an SAI index
    // (some of them require ALLOW FILTERING depending on whether the index is on keys(m), values(m)
    // or entries(m), so if we enable the operators depending on the context, we might want to
    // consider that).
    builder.field(GraphQLInputObjectField.newInputObjectField().name("containsKey").type(keyType));
    builder.field(GraphQLInputObjectField.newInputObjectField().name("contains").type(valueType));
    builder.field(
        GraphQLInputObjectField.newInputObjectField().name("containsEntry").type(entryType));

    return builder.build();
  }

  private GraphQLInputType listFilter(GraphQLInputType gqlInputType) {

    GraphQLList gqlListType = (GraphQLList) gqlInputType;
    GraphQLInputType elementType = (GraphQLInputType) gqlListType.getWrappedType();

    // Basic operators are allowed for:
    // - conditional updates
    // - when the collection column is the primary key
    GraphQLInputObjectType.Builder builder = basicComparisons(gqlInputType);

    // 'contains' is allowed for regular columns with an SAI index
    builder.field(GraphQLInputObjectField.newInputObjectField().name("contains").type(elementType));

    return builder.build();
  }

  private static GraphQLInputObjectType singleElementFilter(GraphQLInputType gqlInputType) {
    return basicComparisons(gqlInputType).build();
  }

  private static GraphQLInputObjectType.Builder basicComparisons(GraphQLInputType gqlInputType) {
    return GraphQLInputObjectType.newInputObject()
        .name(buildFilterName(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("eq").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("notEq").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gt").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("gte").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lt").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("lte").type(gqlInputType))
        .field(GraphQLInputObjectField.newInputObjectField().name("in").type(list(gqlInputType)));
  }

  private static String buildFilterName(GraphQLInputType gqlInputType) {
    return format(gqlInputType) + "FilterInput";
  }

  private static String format(GraphQLInputType gqlInputType) {
    if (gqlInputType instanceof GraphQLScalarType) {
      return ((GraphQLScalarType) gqlInputType).getName();
    } else if (gqlInputType instanceof GraphQLList) {
      // This covers maps as well, since we model them as list of entries
      GraphQLList gqlListType = (GraphQLList) gqlInputType;
      GraphQLInputType elementType = (GraphQLInputType) gqlListType.getWrappedType();
      return "List" + format(elementType);
    } else if (gqlInputType instanceof GraphQLInputObjectType) {
      String name = ((GraphQLInputObjectType) gqlInputType).getName();
      if (name.endsWith("Input")) {
        name = name.substring(0, name.length() - 5);
      }
      return name;
    } else {
      throw new IllegalStateException("Unexpected input type " + gqlInputType);
    }
  }
}
