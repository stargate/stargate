/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.search.solr.core.SolrSchemaComparator;
import java.util.Set;

public class SolrSchemaChangedWarnings {
  static String getWarningsRelatedToSchemaChange(String oldSchema, String newSchema) {
    SolrSchemaComparator comparator = new SolrSchemaComparator(oldSchema, newSchema);
    StringBuilder warning = new StringBuilder();

    if (fullReindexRequired(comparator)) {
      warning.append("Full reindex with deleteAll=true required:\n");
    } else if (incrementalReindexRequired(comparator)) {
      warning.append("Incremental reindex with deleteAll=false required:\n");
    }
    addWarning(warning, "docValues changed for fields: ", comparator.fieldsWithChangedDocValues());
    addWarning(warning, "field type changed for fields: ", comparator.fieldsWithChangedType());
    addWarning(warning, "text field type definition changed: ", comparator.changedTextFieldTypes());
    addWarning(warning, "new fields added: ", comparator.addedFields());

    return warning.toString();
  }

  private static void addWarning(StringBuilder warning, String header, Set<String> fieldNames) {
    if (!fieldNames.isEmpty()) {
      warning.append(header);
      warning.append(fieldNames);
      warning.append("\n");
    }
  }

  private static boolean incrementalReindexRequired(SolrSchemaComparator comparator) {
    return !comparator.addedFields().isEmpty();
  }

  private static boolean fullReindexRequired(SolrSchemaComparator comparator) {
    return !comparator.fieldsWithChangedDocValues().isEmpty()
        || !comparator.fieldsWithChangedType().isEmpty()
        || !comparator.changedTextFieldTypes().isEmpty();
  }
}
