/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/** Interface to map, format and version Solr and Cassandra types. */
public interface CassandraSolrTypeMapper {
  /**
   * Return true if the given Solr type is a valid key type, false otherwise.
   *
   * @param fieldType
   * @return
   */
  boolean isValidKeyType(FieldType fieldType);

  /**
   * Checks if given Cassandra type is either unsupported, or it contains any unsupported type (when
   * given type is a UDT, tuple, or collection type).
   *
   * @return empty option if type is not unsupported, or it contains only supported types. Offending
   *     type is returned otherwise.
   */
  default Optional<? extends AbstractType> checkIfCassandraTypeContainsUnsupportedTypes(
      AbstractType<?> type, String nestedFieldName) {
    return Optional.empty();
  }

  /**
   * Map the given Solr type to its Cassandra type, optionally verifying if the given candidate type
   * is the correct one and returning it if so, otherwise returning the actual mapped type.
   *
   * @param fieldSchema
   * @param fieldType
   * @param candidateType
   * @return
   */
  AbstractType mapToCassandraType(
      SchemaField fieldSchema, FieldType fieldType, AbstractType candidateType);

  /**
   * Map the given Cassandra type to its solr type
   *
   * @param cassandraType
   * @return The Solr Class type or null
   */
  Class<? extends FieldType> mapToSolrType(Class<? extends AbstractType> cassandraType);

  /**
   * Format the given value, with the given Solr type, to its Cassandra value according to the
   * mapped type.
   *
   * @param value
   * @param fieldType
   * @return
   */
  String formatToCassandraType(Object value, FieldType fieldType);

  /**
   * Format the given value, with the given Solr type, to its Solr value according to the mapped
   * type.
   *
   * @param value
   * @param fieldType
   * @return
   */
  String formatToSolrType(String value, FieldType fieldType);

  /**
   * Takes a java value and returns its C* BB
   *
   * @param value
   * @param fieldType
   * @return
   */
  ByteBuffer formatToCassandraBB(Object value, AbstractType fieldType);

  /**
   * Get the type mapping version string implemented by the type mapper.
   *
   * @return
   */
  String getVersion();

  /**
   * Return true if this type mapper is compatible with the given version or if forced, false
   * otherwise.
   *
   * @param version
   * @return
   */
  boolean isCompatibleWith(String version);

  /** Type mappers factory. */
  interface Factory {
    CassandraSolrTypeMapper make(boolean forced);
  }

  /** Enum representing type mapping versions. */
  enum TypeMappingVersion {
    VERSION_1(1),
    VERSION_2(2);
    private final Integer version;

    TypeMappingVersion(Integer version) {
      this.version = version;
    }

    public String getVersion() {
      return version.toString();
    }

    public boolean olderThan(TypeMappingVersion other) {
      return this.version < other.version;
    }

    public static TypeMappingVersion lookup(String version) {
      for (TypeMappingVersion candidate : TypeMappingVersion.values()) {
        if (candidate.getVersion().equals(version)) {
          return candidate;
        }
      }
      throw new IllegalStateException("Wrong type mapping version: " + version);
    }
  }
}
