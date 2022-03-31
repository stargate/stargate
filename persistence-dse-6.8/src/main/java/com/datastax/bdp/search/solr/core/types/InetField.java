/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core.types;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.StrField;

public class InetField extends StrField {
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    // Tokenizing makes no sense
    restrictProps(TOKENIZED);
  }

  @Override
  public InetAddress toObject(IndexableField f) {
    return InetAddresses.forString(f.toString());
  }
}
