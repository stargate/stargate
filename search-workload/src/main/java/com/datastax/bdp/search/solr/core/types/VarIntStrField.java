/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import java.math.BigInteger;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;

public class VarIntStrField extends StrField {
  @Override
  public BigInteger toObject(IndexableField f) {
    return new BigInteger((String) super.toObject(f));
  }

  @Override
  public BigInteger toObject(SchemaField sf, BytesRef term) {
    return new BigInteger((String) super.toObject(sf, term));
  }
}
