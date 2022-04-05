/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import com.datastax.bdp.search.solr.Cql3Utils;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.*;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;

public class TupleField extends StrField {
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  static {
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addSerializer(TupleTypeAndBytes.class, new TupleTypeSerializer());
    JSON_MAPPER.registerModule(simpleModule);
  }

  public static ByteBuffer fromJsonToBytes(
      String fieldName,
      TupleType tupleType,
      String tupleJson,
      IndexSchema indexSchema,
      CassandraSolrTypeMapper typeMapper)
      throws IOException {
    Map<String, Object> tupleMap = JSON_MAPPER.readValue(tupleJson, MAP_TYPE);
    return fromMapToBytes(fieldName, tupleType, tupleMap, indexSchema, typeMapper);
  }

  public static String fromBytesToJson(
      TupleType tupleType, ByteBuffer tupleBytes, CassandraSolrTypeMapper typeMapper)
      throws IOException {
    return JSON_MAPPER.writeValueAsString(new TupleTypeAndBytes(tupleType, tupleBytes, typeMapper));
  }

  private static ByteBuffer fromMapToBytes(
      String fieldName,
      TupleType tupleType,
      Map<String, Object> tupleMap,
      IndexSchema indexSchema,
      CassandraSolrTypeMapper typeMapper)
      throws IOException {
    ByteBuffer[] tupleValues = new ByteBuffer[tupleType.size()];
    for (int i = 0; i < tupleType.size(); i++) {
      String subFieldName =
          tupleType instanceof UserType
              ? ((UserType) tupleType).fieldName(i).toString()
              : "field" + (i + 1);
      String fullFieldName = fieldName + "." + subFieldName;

      AbstractType currentType = tupleType.type(i);
      SchemaField schemaField = indexSchema.getFieldOrNull(fullFieldName);
      Object fieldValue = tupleMap.get(subFieldName);

      if (schemaField == null || fieldValue == null) {
        tupleValues[i] = null;
      } else if (currentType instanceof TupleType) {
        tupleValues[i] =
            fromMapToBytes(
                fullFieldName,
                (TupleType) currentType,
                (Map<String, Object>) fieldValue,
                indexSchema,
                typeMapper);
      } else if (!tupleType.type(i).isCollection()) {
        String cassandraValue =
            typeMapper.formatToCassandraType(fieldValue.toString(), schemaField.getType());
        tupleValues[i] = currentType.fromString(cassandraValue);
      } else {
        AbstractType collectionType = tupleType.type(i);

        if (collectionType instanceof MapType) {
          AbstractType valueType = ((MapType) collectionType).getValuesType();
          Map values = new HashMap<>();
          for (Map.Entry current : ((Map<String, Object>) fieldValue).entrySet()) {
            String cassandraValue =
                typeMapper.formatToCassandraType(
                    current.getValue().toString(), schemaField.getType());
            values.put(current.getKey(), valueType.compose(valueType.fromString(cassandraValue)));
          }

          tupleValues[i] = currentType.decompose(values);
        } else {
          AbstractType elementType = null;
          Collection values = null;
          if (collectionType instanceof ListType) {
            elementType = ((ListType) collectionType).getElementsType();
            values = new LinkedList<>();
          } else if (collectionType instanceof SetType) {
            elementType = ((SetType) collectionType).getElementsType();
            values = new HashSet<>();
          } else {
            throw new IllegalStateException(
                "Invalid collection validator: " + collectionType.getClass());
          }

          for (Object current : (List) fieldValue) {
            if (elementType instanceof TupleType) {
              values.add(
                  fromMapToBytes(
                      fullFieldName,
                      (TupleType) elementType,
                      (Map<String, Object>) current,
                      indexSchema,
                      typeMapper));
            } else {
              String cassandraValue =
                  typeMapper.formatToCassandraType(current.toString(), schemaField.getType());
              if (cassandraValue != null) {
                values.add(elementType.compose(elementType.fromString(cassandraValue)));
              }
            }
          }

          tupleValues[i] = currentType.decompose(values);
        }
      }
    }

    return TupleType.buildValue(tupleValues);
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    restrictProps(TOKENIZED);
  }

  private static class TupleTypeAndBytes {
    public final TupleType type;
    public final ByteBuffer bytes;
    public final CassandraSolrTypeMapper typeMapper;

    public TupleTypeAndBytes(TupleType type, ByteBuffer bytes, CassandraSolrTypeMapper typeMapper) {
      this.type = type;
      this.bytes = bytes;
      if (typeMapper == null) {
        throw new IllegalArgumentException("The supplied type mapper can't be null");
      }
      this.typeMapper = typeMapper;
    }
  }

  private static class TupleTypeSerializer extends JsonSerializer<TupleTypeAndBytes> {

    private static String getSolrString(
        CassandraSolrTypeMapper typeMapper, AbstractType<?> cassandraType, Object value) {
      return getSolrString(typeMapper, cassandraType, value, null);
    }

    private static String getSolrString(
        CassandraSolrTypeMapper typeMapper,
        AbstractType<?> cassandraType,
        Object value,
        FieldType ft) {
      ft = ft == null ? getSolrFieldType(typeMapper, cassandraType) : ft;
      return typeMapper.formatToSolrType(Cql3Utils.typeToString(cassandraType, value), ft);
    }

    private static FieldType getSolrFieldType(
        CassandraSolrTypeMapper typeMapper, AbstractType<?> cassandraType) {
      FieldType ft = null;
      try {
        ft = typeMapper.mapToSolrType(cassandraType.getClass()).newInstance();
      } catch (Exception e) {
        throw new IllegalStateException(
            "A problem was found while json serializing a value to its solr representation", e);
      }

      return ft;
    }

    @Override
    public void serialize(
        TupleTypeAndBytes input, JsonGenerator generator, SerializerProvider provider)
        throws IOException {
      TupleType tupleType = input.type;
      ByteBuffer tupleBytes = input.bytes;
      ByteBuffer[] tupleValues = tupleType.split(tupleBytes);

      generator.writeStartObject();

      for (int i = 0; i < tupleValues.length; i++) {
        AbstractType currentType = tupleType.type(i);
        ByteBuffer currentValue = tupleValues[i];
        if (currentValue != null) {
          String fieldName =
              tupleType instanceof UserType
                  ? ColumnIdentifier.getInterned(
                          ((UserType) tupleType).fieldName(i).toString(), true)
                      .toCQLString()
                  : "field" + (i + 1);

          if (currentType instanceof TupleType) {
            generator.writeFieldName(fieldName);
            generator.writeObject(
                new TupleTypeAndBytes((TupleType) currentType, currentValue, input.typeMapper));
          } else if (tupleType.type(i).isCollection()) {
            CollectionType collectionType = (CollectionType) tupleType.type(i);
            AbstractType elementValidator = null;
            switch (collectionType.kind) {
              case SET:
                SetType setType = (SetType) collectionType;
                elementValidator = setType.getElementsType();
                Set setValues = (Set) setType.compose(currentValue);

                if (setValues != null) {
                  if (elementValidator instanceof TupleType) {
                    Set<TupleTypeAndBytes> tupleTypeSet = new HashSet<>();
                    for (Object value : setValues) {
                      tupleTypeSet.add(
                          new TupleTypeAndBytes(
                              (TupleType) elementValidator, (ByteBuffer) value, input.typeMapper));
                    }
                    setValues = tupleTypeSet;
                  } else {
                    Set<String> solrStringsSetValues = new HashSet<>();
                    FieldType ft = getSolrFieldType(input.typeMapper, elementValidator);
                    for (Object value : setValues) {
                      solrStringsSetValues.add(
                          getSolrString(input.typeMapper, elementValidator, value, ft));
                    }
                    setValues = solrStringsSetValues;
                  }
                }

                generator.writeFieldName(fieldName);
                generator.writeObject(setValues);
                break;
              case LIST:
                ListType listType = (ListType) collectionType;
                elementValidator = listType.getElementsType();
                List listValues = (List) listType.compose(currentValue);

                if (listValues != null) {
                  if (elementValidator instanceof TupleType) {
                    List<TupleTypeAndBytes> tupleTypeList = new ArrayList<>();
                    for (Object value : listValues) {
                      tupleTypeList.add(
                          new TupleTypeAndBytes(
                              (TupleType) elementValidator, (ByteBuffer) value, input.typeMapper));
                    }
                    listValues = tupleTypeList;
                  } else {
                    List<String> solrStringsListValues = new ArrayList<>();
                    FieldType ft = getSolrFieldType(input.typeMapper, elementValidator);
                    for (Object value : listValues) {
                      solrStringsListValues.add(
                          getSolrString(input.typeMapper, elementValidator, value, ft));
                    }
                    listValues = solrStringsListValues;
                  }
                }

                generator.writeFieldName(fieldName);
                generator.writeObject(listValues);
                break;
              case MAP:
                MapType mapType = (MapType) collectionType;
                AbstractType valueComparator = mapType.valueComparator();
                Map mapValues = (Map) mapType.compose(currentValue);

                if (mapValues != null) {
                  if (valueComparator instanceof TupleType) {
                    Map<String, TupleTypeAndBytes> tupleTypeMap = new HashMap<>();
                    for (Object key : mapValues.entrySet()) {
                      tupleTypeMap.put(
                          (String) key,
                          new TupleTypeAndBytes(
                              (TupleType) valueComparator,
                              (ByteBuffer) mapValues.get(key),
                              input.typeMapper));
                    }
                    mapValues = tupleTypeMap;
                  } else {
                    Map<String, String> solrStringsMap = new HashMap<>();
                    FieldType ft = getSolrFieldType(input.typeMapper, valueComparator);
                    for (Object value : mapValues.entrySet()) {
                      Map.Entry<String, Object> current = (Map.Entry<String, Object>) value;
                      solrStringsMap.put(
                          current.getKey(),
                          getSolrString(input.typeMapper, valueComparator, current.getValue(), ft));
                    }
                    mapValues = solrStringsMap;
                  }
                }

                generator.writeFieldName(fieldName);
                generator.writeObject(mapValues);
                break;
              default:
                throw new IllegalStateException("Unknown collection type: " + collectionType);
            }
          } else {
            generator.writeFieldName(fieldName);
            generator.writeObject(
                getSolrString(input.typeMapper, tupleType.type(i), tupleValues[i]));
          }
        }
      }

      generator.writeEndObject();
    }
  }
}
