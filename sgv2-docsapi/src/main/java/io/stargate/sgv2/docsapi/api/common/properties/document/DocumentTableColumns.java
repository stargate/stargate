package io.stargate.sgv2.docsapi.api.common.properties.document;

import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import java.util.List;
import java.util.Set;

/** Helper for understanding the available document table columns. */
public interface DocumentTableColumns {

  /** @return All columns as the {@link ImmutableColumn} representation. */
  List<Column> allColumns();

  /** @return Value columns, including the leaf. */
  Set<String> valueColumnNames();

  /** @return All the JSON path columns based on the max depth. */
  Set<String> pathColumnNames();

  /** @return All the columns of the document table. */
  Set<String> allColumnNames();
}
