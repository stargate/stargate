/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.utils.Hex;
import org.apache.solr.common.util.Base64;
import org.apache.solr.schema.*;
import org.apache.solr.util.DateMathParser;

/** Map and format Solr and Cassandra types, using version 1 mappings. */
@SuppressWarnings("deprecation")
public class V1TypeMapper implements CassandraSolrTypeMapper {
  protected final Set<Class> keyTypes =
      Sets.newHashSet(
          BoolField.class,
          StrField.class,
          TextField.class,
          TrieDateField.class,
          TrieDoubleField.class,
          TrieFloatField.class,
          TrieIntField.class,
          TrieLongField.class,
          InetField.class,
          UUIDField.class,
          TimeUUIDField.class,
          EnumField.class,
          TrieField.class,
          AsciiStrField.class,
          VarIntStrField.class,
          DecimalStrField.class);

  protected final Map<Class, AbstractType> solrToCassandraTypes =
      Maps.newHashMap(
          ImmutableMap.<Class, AbstractType>builder()
              .put(BinaryField.class, BytesType.instance)
              .put(BoolField.class, BooleanType.instance)
              .put(TrieDateField.class, DateType.instance)
              .put(TrieDoubleField.class, DoubleType.instance)
              .put(TrieFloatField.class, FloatType.instance)
              .put(TrieIntField.class, Int32Type.instance)
              .put(TrieLongField.class, LongType.instance)
              .put(DecimalStrField.class, DecimalType.instance)
              .put(UUIDField.class, UUIDType.instance)
              .put(TimeUUIDField.class, TimeUUIDType.instance)
              .put(InetField.class, InetAddressType.instance)
              .put(AsciiStrField.class, AsciiType.instance)
              .put(VarIntStrField.class, IntegerType.instance)
              .build());

  @SuppressWarnings("unchecked")
  protected final Map<AbstractType, Set<AbstractType>> compatibleTypes =
      Maps.newHashMap(
          ImmutableMap.<AbstractType, Set<AbstractType>>builder()
              .put(UUIDType.instance, Sets.newHashSet(TimeUUIDType.instance))
              .put(DateType.instance, Sets.newHashSet(TimestampType.instance))
              .put(Int32Type.instance, Sets.newHashSet(ShortType.instance, ByteType.instance))
              .build());

  @SuppressWarnings("unchecked")
  protected final Map<TrieField.TrieTypes, Class> trieTypes =
      Maps.newHashMap(
          ImmutableMap.<TrieField.TrieTypes, Class>builder()
              .put(TrieField.TrieTypes.DATE, TrieDateField.class)
              .put(TrieField.TrieTypes.DOUBLE, TrieDoubleField.class)
              .put(TrieField.TrieTypes.FLOAT, TrieFloatField.class)
              .put(TrieField.TrieTypes.INTEGER, TrieIntField.class)
              .put(TrieField.TrieTypes.LONG, TrieLongField.class)
              .build());

  protected final boolean forced;

  protected V1TypeMapper(boolean forced) {
    this.forced = forced;
  }

  @Override
  public ByteBuffer formatToCassandraBB(Object value, AbstractType cassandraType) {
    cassandraType = cassandraType.asCQL3Type().getType();
    try {
      ByteBuffer res = null;
      if (value instanceof String) {
        res = cassandraType.fromString((String) value);
      } else if (cassandraType instanceof BytesType && value instanceof byte[]) {
        res = ByteBuffer.wrap((byte[]) value);
      } else if (cassandraType instanceof SimpleDateType && value instanceof Date) {
        res = SimpleDateType.instance.fromTimeInMillis(((Date) value).getTime());
      } else if (cassandraType instanceof ShortType && value instanceof Integer) {
        res = cassandraType.getSerializer().serialize(new Short(value.toString()));
      } else if (cassandraType instanceof ByteType && value instanceof Integer) {
        res = cassandraType.getSerializer().serialize(((Integer) value).byteValue());
      } else {
        res = cassandraType.getSerializer().serialize(value);
      }

      return res;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed processing field with value " + value + " of type " + cassandraType, e);
    }
  }

  @Override
  public boolean isValidKeyType(FieldType solrType) {
    return keyTypes.contains(solrType.getClass());
  }

  @Override
  public AbstractType mapToCassandraType(
      SchemaField fieldSchema, FieldType solrType, AbstractType candidateType) {
    AbstractType mappedType = null;
    // Multi-value types are serialized to json, so they're just strings:
    if (fieldSchema.multiValued() || solrType.isMultiValued()) {
      mappedType = UTF8Type.instance;
    }
    // Otherwise use actual types:
    else {
      mappedType = findCassandraBaseMapping(solrType);
    }

    mappedType = checkCassandraDefaultMapping(mappedType);
    mappedType = checkCassandraCompatibleMapping(mappedType, candidateType);
    return mappedType;
  }

  @Override
  public Class<? extends FieldType> mapToSolrType(Class<? extends AbstractType> cassandraType) {
    throw new UnsupportedOperationException("This operation is not supported in V1 mappers");
  }

  @Override
  public String formatToCassandraType(Object value, FieldType fieldType) {
    if (fieldType instanceof BinaryField) {
      // We accept either Base64-encoded strings...
      if (value.getClass() == String.class) {
        // TODO: Once we no longer support HTTP updates, it may not be possible to get a string
        // here.
        byte[] bytes = Base64.base64ToByteArray((String) value);
        return Hex.bytesToHex(bytes);
      }
      // ...or plain byte arrays.
      return Hex.bytesToHex((byte[]) value);
    }

    String textualValue = value.toString();

    // true if starts with t or T or 1:
    if (fieldType.getClass() == BoolField.class) {
      return (textualValue.startsWith("T")
              || textualValue.startsWith("t")
              || textualValue.startsWith("1"))
          ? "true"
          : "false";
    }

    // solr sometimes takes "NEW" for UUID field, so we need use solr internal to generate the
    // correct UUID:
    if (fieldType.getClass() == UUIDField.class) {
      return fieldType.toInternal(textualValue);
    }

    /** * Dates ** */
    if (value instanceof Date) {
      return String.valueOf(((Date) value).getTime());
    }
    // solr Zulu time to timestamp:
    if (fieldType.getClass() == TrieDateField.class
        || fieldType.getClass() == TrieField.class
            && ((TrieField) fieldType).getType() == TrieField.TrieTypes.DATE) {
      if (!textualValue.isEmpty()) {
        return String.valueOf(DateMathParser.parseMath(null, textualValue).getTime());
      } else {
        return null;
      }
    }

    if (fieldType instanceof SimpleDateField && value instanceof Integer) {
      return SimpleDateType.instance.getSerializer().toString((Integer) value);
    } else if (fieldType instanceof SimpleDateField && value instanceof String) {
      try {
        int days =
            SimpleDateSerializer.timeInMillisToDay(Instant.parse((String) value).toEpochMilli());
        return SimpleDateType.instance.getSerializer().toString(days);
      } catch (DateTimeParseException ignored) {
        // If the date is already C* formatted (yyyy-MM-dd), let it pass through unchanged.
      }
    }

    /** * Binary ** */
    // from Base64 to Hex:
    if (fieldType instanceof BinaryField) {
      byte[] bytes =
          value instanceof String ? Base64.base64ToByteArray((String) value) : (byte[]) value;
      String hex = Hex.bytesToHex(bytes);
      return hex;
    }

    return textualValue;
  }

  @Override
  public String formatToSolrType(String value, FieldType fieldType) {
    // convert C* time to solr Zulu time:
    if (fieldType.getClass() == TrieDateField.class
        || fieldType.getClass() == TrieField.class
            && ((TrieField) fieldType).getType() == TrieField.TrieTypes.DATE) {
      if (!value.isEmpty()) {
        try {
          return Instant.ofEpochMilli(Long.parseLong(value)).toString();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      } else {
        return null;
      }
    }

    // converting from Hex to Base64
    if (fieldType instanceof BinaryField) {
      byte[] bytes = Hex.hexToBytes(value);
      return Base64.byteArrayToBase64(bytes, 0, bytes.length);
    }
    return value;
  }

  @Override
  public String getVersion() {
    return TypeMappingVersion.VERSION_1.getVersion();
  }

  @Override
  public boolean isCompatibleWith(String version) {
    if (!forced) {
      TypeMappingVersion candidate = TypeMappingVersion.lookup(version);
      return candidate.equals(TypeMappingVersion.VERSION_1);
    } else {
      return true;
    }
  }

  protected final AbstractType findCassandraBaseMapping(FieldType solrType) {
    AbstractType mappedType;
    Class clazz = solrType.getClass();
    if (clazz.equals(TrieField.class)) {
      clazz = trieTypes.get(((TrieField) solrType).getType());
    }
    mappedType = solrToCassandraTypes.get(clazz);
    return mappedType;
  }

  protected final AbstractType checkCassandraDefaultMapping(AbstractType mappedType) {
    // Eventually use the default UTF8Type:
    mappedType = mappedType == null ? UTF8Type.instance : mappedType;
    return mappedType;
  }

  protected final AbstractType checkCassandraCompatibleMapping(
      AbstractType mappedType, AbstractType candidateType) {
    // Finally verify if mapping is compatible with the candidate one:
    Set<AbstractType> compatibleMappings = compatibleTypes.get(mappedType);
    if (compatibleMappings != null
        && candidateType != null
        && compatibleMappings.contains(candidateType)) {
      mappedType = candidateType;
    }
    return mappedType;
  }

  public static class Factory implements CassandraSolrTypeMapper.Factory {
    @Override
    public CassandraSolrTypeMapper make(boolean forced) {
      return new V1TypeMapper(forced);
    }
  }
}
