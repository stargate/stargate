package io.stargate.db.schema;

/**
 * Actual type definition for {@code Column.Type.Vector}: needed because we need to pass
 * "dimensions" parameter, and cannot quite use {@link ParameterizedType} since parameterization is
 * not by type (yet at least?) but by fixed length.
 */
public class VectorType extends Column.DelegatingColumnType {
  private final String elementTypeName;

  private final int dimensions;

  private final String marshalTypeName;

  protected VectorType(String elementTypeName, int dimensions) {
    super(Column.Type.Vector);
    this.elementTypeName = elementTypeName;
    this.dimensions = dimensions;
    marshalTypeName =
        String.format(
            "org.apache.cassandra.db.marshal.VectorType(%s, %s)", elementTypeName, dimensions);
  }

  public static VectorType of(String elementTypeName, int dimensions) {
    return new VectorType(elementTypeName, dimensions);
  }

  @Override
  public String marshalTypeName() {
    return marshalTypeName;
  }

  @Override
  public int schemaHashCode() {
    return delegate.schemaHashCode() ^ SchemaHashable.hashCode(elementTypeName) + dimensions;
  }
}
