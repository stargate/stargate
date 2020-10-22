package io.stargate.graphql.schema;

import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableListType;
import java.util.HashMap;
import java.util.Map;

/**
 * Caches a category of GraphQL field types, corresponding to table columns or UDT fields.
 *
 * <p>There are different categories, each implemented by a subclass. Note that this cache does not
 * contain table types, they are handled as top-level entities in {@link DmlSchemaBuilder}.
 *
 * @param <GraphqlT> the returned GraphQL type.
 */
abstract class FieldTypeCache<GraphqlT> {

  protected final NameMapping nameMapping;
  private final Map<ColumnType, GraphqlT> types = new HashMap<>();

  FieldTypeCache(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  GraphqlT get(ColumnType type) {
    type = normalize(type);
    return types.computeIfAbsent(type, this::compute);
  }

  private ColumnType normalize(ColumnType type) {
    // Frozen-ness does not matter. We want frozen and non-frozen versions of a CQL type to be
    // mapped to the same GraphQL type.
    type = type.frozen(false);

    // CQL set and list are both converted to GraphQL list. Avoid creating duplicate types.
    if (type.isSet()) {
      type = ImmutableListType.builder().addAllParameters(type.parameters()).build();
    }

    if (type == Type.Varchar) {
      // Currently field type cache does not tolerate 2 different column types (Text and Varchar)
      // using the same graphql type, workaround by using a single column type
      type = Type.Text;
    }

    return type;
  }

  /**
   * Computes a result on a cache miss. If you need nested types, use {@link #get} to obtain them,
   * in case they were already cached.
   */
  protected abstract GraphqlT compute(ColumnType columnType);

  protected GraphQLScalarType getScalar(Type type) {
    switch (type) {
      case Ascii:
        return CustomScalar.ASCII.getGraphQLScalar();
      case Bigint:
        return CustomScalar.BIGINT.getGraphQLScalar();
      case Blob:
        return CustomScalar.BLOB.getGraphQLScalar();
      case Boolean:
        return Scalars.GraphQLBoolean;
      case Counter:
        return CustomScalar.COUNTER.getGraphQLScalar();
      case Decimal:
        return CustomScalar.DECIMAL.getGraphQLScalar();
      case Double:
        // GraphQL's Float is a signed double‐precision fractional value
        return Scalars.GraphQLFloat;
      case Duration:
        return CustomScalar.DURATION.getGraphQLScalar();
      case Float:
        // Use a custom scalar named "Float32"
        return CustomScalar.FLOAT.getGraphQLScalar();
      case Int:
        return Scalars.GraphQLInt;
      case Smallint:
        return CustomScalar.SMALLINT.getGraphQLScalar();
      case Tinyint:
        return CustomScalar.TINYINT.getGraphQLScalar();
      case Text:
      case Varchar:
        return Scalars.GraphQLString;
      case Timestamp:
        return CustomScalar.TIMESTAMP.getGraphQLScalar();
      case Uuid:
        return CustomScalar.UUID.getGraphQLScalar();
      case Varint:
        return CustomScalar.VARINT.getGraphQLScalar();
      case Timeuuid:
        return CustomScalar.TIMEUUID.getGraphQLScalar();
      case Inet:
        return CustomScalar.INET.getGraphQLScalar();
      case Date:
        return CustomScalar.DATE.getGraphQLScalar();
      case Time:
        return CustomScalar.TIME.getGraphQLScalar();
      default:
        throw new IllegalArgumentException("Unsupported primitive type " + type);
    }
  }
}
