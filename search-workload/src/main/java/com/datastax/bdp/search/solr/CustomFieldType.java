/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr;

import org.apache.solr.schema.FieldType;

/** Solr custom types must implement this interface to be correctly mapped to/from Cassandra. */
public interface CustomFieldType {
  /**
   * Get the Solr FieldType used to obtain the stored value: it can be different from the actually
   * indexed type (represented by this custom type), but needs to match the corresponding Cassandra
   * type for the field.
   */
  FieldType getStoredFieldType();
}
