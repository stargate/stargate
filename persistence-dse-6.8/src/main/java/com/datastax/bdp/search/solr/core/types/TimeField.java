/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import java.util.List;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieLongField;

public class TimeField extends TrieLongField {
  public String toTrieLongFieldFormat(Object value) {
    Long longValue = null;

    if (value instanceof Long) {
      longValue = (Long) value;
    } else if (value instanceof String) {
      longValue = ByteBufferUtil.toLong(TimeType.instance.fromString((String) value));
    } else {
      throw new IllegalArgumentException("Can only build a TimeField out of a Long or a String");
    }

    return Long.toString(longValue);
  }

  @Override
  public String readableToIndexed(String val) {
    return super.readableToIndexed(toTrieLongFieldFormat(val));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    super.readableToIndexed(toTrieLongFieldFormat(val.toString()), result);
  }

  @Override
  public String toInternal(String val) {
    return super.toInternal(toTrieLongFieldFormat(val));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    value = toTrieLongFieldFormat(value);
    return super.createField(field, value, boost);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    value = toTrieLongFieldFormat(value);
    return super.createFields(field, value, boost);
  }

  @Override
  public Class<? extends FieldType> getKnownType() {
    // Solr's binary codec knows how to serialize TrieDateField.
    return TrieLongField.class;
  }
}
