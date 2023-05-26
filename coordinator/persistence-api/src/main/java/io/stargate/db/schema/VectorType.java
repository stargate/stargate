package io.stargate.db.schema;

/**
 * Actual type definition for {@code Column.Type.Vector}: needed because we need to pass
 * "dimensions" parameter, and cannot quite use {@link ParameterizedType} since parameterization is
 * not by type (yet at least?) but by fixed length.
 */
public class VectorType extends Column.DelegatingColumnType {
  private final Column.ColumnType elementType;
  private final int dimensions;

  private final String marshalTypeName;

  protected VectorType(Column.ColumnType elementType, int dimensions) {
    super(Column.Type.Vector);
    this.elementType = elementType;
    this.dimensions = dimensions;
    marshalTypeName =
        String.format(
            "org.apache.cassandra.db.marshal.VectorType(%s, %s)",
            elementType.marshalTypeName(), dimensions);
  }

  public static VectorType of(Column.ColumnType elementType, int dimensions) {
    return new VectorType(elementType, dimensions);
  }

  @Override
  public String marshalTypeName() {
    return marshalTypeName;
  }

  @Override
  public int schemaHashCode() {
    return delegate.schemaHashCode() ^ elementType.schemaHashCode() + dimensions;
  }
}
