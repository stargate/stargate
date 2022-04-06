/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import java.util.List;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;

public class SimpleDateField extends TrieDateField {
  public static final String TIME_SUFFIX = "T00:00:00Z";

  /*
   * Necessary to make it parsable by the TrieDateField.
   *
   * Cql 'date' field format is YYYY-MM-DD and this is not parsable by TrieDateField.
   * Here we add the missing bits to make it parsable
   */
  public String toTrieFieldDateFormat(Object value) {
    if (value instanceof String && ((String) value).endsWith("T00:00:00Z")) {
      return (String) value;
    }

    Integer intValue = null;

    if (value instanceof Integer) {
      intValue = (Integer) value;
    } else if (value instanceof String) {
      intValue = ByteBufferUtil.toInt(SimpleDateType.instance.fromString((String) value));
    } else {
      throw new IllegalArgumentException(
          "Can only build a SimpleDateField out of an Integer or String");
    }

    String strValue = SimpleDateType.instance.getSerializer().toString(intValue);
    return strValue + TIME_SUFFIX;
  }

  @Override
  public String readableToIndexed(String val) {
    return super.readableToIndexed(toTrieFieldDateFormat(val));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    super.readableToIndexed(toTrieFieldDateFormat(val.toString()), result);
  }

  @Override
  public String toInternal(String val) {
    return super.toInternal(toTrieFieldDateFormat(val));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    value = toTrieFieldDateFormat(value);
    return super.createField(field, value, boost);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    value = toTrieFieldDateFormat(value);
    return super.createFields(field, value, boost);
  }

  @Override
  public Class<? extends FieldType> getKnownType() {
    // Solr's binary codec knows how to serialize TrieDateField.
    return TrieDateField.class;
  }
}
