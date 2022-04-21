package io.stargate.sgv2.docsapi.api.common.properties.document;

/** Helper for understanding the available document table columns. */
public interface DocumentTableColumns {

  /** @return Value columns, including the leaf. */
  String[] valueColumnNames();

  /** @return All the JSON path columns based on the max depth. */
  String[] pathColumnNames();

  /** @return All the columns of the document table. */
  String[] allColumnNames();
}
