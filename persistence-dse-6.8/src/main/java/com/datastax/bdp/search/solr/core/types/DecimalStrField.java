/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import java.math.BigDecimal;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;

public class DecimalStrField extends StrField {
  @Override
  public BigDecimal toObject(IndexableField f) {
    return new BigDecimal((String) super.toObject(f));
  }

  @Override
  public BigDecimal toObject(SchemaField sf, BytesRef term) {
    return new BigDecimal((String) super.toObject(sf, term));
  }
}
