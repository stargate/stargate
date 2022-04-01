/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import com.datastax.bdp.search.solr.CustomFieldType;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.PointType;
import org.apache.solr.schema.*;

/** Map and format Solr and Cassandra types, using version 2 mappings. */
public class V2TypeMapper extends V1TypeMapper {
  @SuppressWarnings("unchecked")
  protected final Map<Class<? extends FieldType>, Class<? extends AbstractType>>
      solrToCassandraSuperTypes =
          Maps.newHashMap(
              ImmutableMap.<Class<? extends FieldType>, Class<? extends AbstractType>>builder()
                  .put(TupleField.class, TupleType.class)
                  .put(SpatialRecursivePrefixTreeFieldType.class, AbstractGeometricType.class)
                  .build());

  @SuppressWarnings("unchecked")
  protected final Map<Class<? extends AbstractType>, Class<? extends FieldType>>
      cassandraToSolrTypes =
          Maps.newHashMap(
              ImmutableMap.<Class<? extends AbstractType>, Class<? extends FieldType>>builder()
                  .put(BytesType.class, BinaryField.class)
                  .put(UTF8Type.class, StrField.class)
                  .put(ShortType.class, TrieIntField.class) // smallint
                  .put(ByteType.class, TrieIntField.class) // tinyint
                  .put(Int32Type.class, TrieIntField.class)
                  .put(LongType.class, TrieLongField.class)
                  .put(UUIDType.class, UUIDField.class)
                  .put(TimeUUIDType.class, TimeUUIDField.class)
                  .put(DateType.class, TrieDateField.class)
                  .put(BooleanType.class, BoolField.class)
                  .put(FloatType.class, TrieFloatField.class)
                  .put(DoubleType.class, TrieDoubleField.class)
                  .put(TimestampType.class, TrieDateField.class)
                  .put(DateRangeType.class, DateRangeField.class)
                  .put(SimpleDateType.class, SimpleDateField.class)
                  .put(TimeType.class, TimeField.class)
                  .put(InetAddressType.class, InetField.class)
                  .put(AsciiType.class, AsciiStrField.class)
                  .put(DecimalType.class, DecimalStrField.class)
                  .put(IntegerType.class, VarIntStrField.class)
                  .put(TupleType.class, TupleField.class)
                  .put(UserType.class, TupleField.class)
                  .put(PointType.class, SpatialRecursivePrefixTreeFieldType.class)
                  .put(LineStringType.class, SpatialRecursivePrefixTreeFieldType.class)
                  .put(PolygonType.class, SpatialRecursivePrefixTreeFieldType.class)
                  .build());

  private final List<Class<? extends AbstractType>> unsupportedCassandraTypes =
      ImmutableList.of(DurationType.class);

  private V2TypeMapper(boolean forced) {
    super(forced);
    initSolrToCassandraTypes();
    initSolrKeyTypes();
  }

  @Override
  public AbstractType mapToCassandraType(
      SchemaField fieldSchema, FieldType solrType, AbstractType candidateType) {
    Class<? extends AbstractType> cassandraSuperType =
        solrToCassandraSuperTypes.get(solrType.getClass());

    if (cassandraSuperType != null
        && cassandraSuperType.isAssignableFrom(candidateType.asCQL3Type().getType().getClass())) {
      return candidateType;
    }

    if (solrType instanceof CustomFieldType) {
      solrType = ((CustomFieldType) solrType).getStoredFieldType();
    }
    AbstractType mappedType = findCassandraBaseMapping(solrType);
    mappedType = checkCassandraDefaultMapping(mappedType);
    mappedType = checkCassandraCompatibleMapping(mappedType, candidateType);
    return mappedType;
  }

  @Override
  public Class<? extends FieldType> mapToSolrType(Class<? extends AbstractType> cassandraType) {
    return cassandraToSolrTypes.get(cassandraType);
  }

  @Override
  public String formatToCassandraType(Object value, FieldType solrType) {
    if (solrType instanceof CustomFieldType) {
      return super.formatToCassandraType(value, ((CustomFieldType) solrType).getStoredFieldType());
    } else {
      return super.formatToCassandraType(value, solrType);
    }
  }

  @Override
  public String formatToSolrType(String value, FieldType solrType) {
    FieldType actualFieldType =
        solrType instanceof CustomFieldType
            ? ((CustomFieldType) solrType).getStoredFieldType()
            : solrType;

    if (actualFieldType instanceof TimeField) {
      return ((TimeField) actualFieldType).toTrieLongFieldFormat(value);
    } else if (actualFieldType instanceof SimpleDateField) {
      return ((SimpleDateField) actualFieldType).toTrieFieldDateFormat(value);
    } else {
      return super.formatToSolrType(value, actualFieldType);
    }
  }

  @Override
  public String getVersion() {
    return TypeMappingVersion.VERSION_2.getVersion();
  }

  @Override
  public boolean isCompatibleWith(String version) {
    if (!forced) {
      TypeMappingVersion candidate = TypeMappingVersion.lookup(version);
      return candidate.equals(TypeMappingVersion.VERSION_2);
    } else {
      return true;
    }
  }

  @Override
  public Optional<? extends AbstractType> checkIfCassandraTypeContainsUnsupportedTypes(
      AbstractType<?> type, String nestedFieldName) {
    if (type == null) {
      return Optional.empty();
    }
    AbstractType<?> unwrappedType = type.asCQL3Type().getType();
    if (SchemaTool.isTupleOrTupleCollection(unwrappedType)) {
      AbstractType<?> nestedType =
          SchemaTool.getTupleSubFieldType(nestedFieldName, unwrappedType, true);
      return Optional.of(nestedType).filter(t -> unsupportedCassandraTypes.contains(t.getClass()));
    } else if (unwrappedType.isCollection()) {
      return checkIfCassandraTypeContainsUnsupportedTypes(
          SchemaTool.getElementType((CollectionType<?>) unwrappedType), nestedFieldName);
    }

    return Optional.of(unwrappedType).filter(t -> unsupportedCassandraTypes.contains(t.getClass()));
  }

  @SuppressWarnings("deprecation")
  private void initSolrToCassandraTypes() {
    solrToCassandraTypes.put(TrieDateField.class, TimestampType.instance);
    solrToCassandraTypes.put(SimpleDateField.class, SimpleDateType.instance);
    solrToCassandraTypes.put(TimeField.class, TimeType.instance);
    solrToCassandraTypes.put(DateRangeField.class, DateRangeType.instance);
  }

  private void initSolrKeyTypes() {
    keyTypes.add(SimpleDateField.class);
    keyTypes.add(TimeField.class);
    keyTypes.add(DateRangeField.class);
  }

  public static class Factory implements CassandraSolrTypeMapper.Factory {
    @Override
    public CassandraSolrTypeMapper make(boolean forced) {
      return new V2TypeMapper(forced);
    }
  }
}
