package io.stargate.db.schema;

/**
 * Actual type definition for {@code Column.Type.Vector}: needed because we need to pass
 * "dimensions" parameter, and cannot quite use {@link ParameterizedType} since parameterization is
 * not by type (yet at least?) but by fixed length.
 */
public class VectorType extends Column.DelegatingColumnType {
  private final int dimensions;

  private final String marshalTypeName;

  protected VectorType(int dimensions) {
    super(Column.Type.Vector);
    this.dimensions = dimensions;
    marshalTypeName = String.format("org.apache.cassandra.db.marshal.VectorType(%s)", dimensions);
  }

  public static VectorType of(int dimensions) {
    return new VectorType(dimensions);
  }

  @Override
  public String marshalTypeName() {
    return marshalTypeName;
  }

  @Override
  public int schemaHashCode() {
    return delegate.schemaHashCode() + dimensions;
  }
}
