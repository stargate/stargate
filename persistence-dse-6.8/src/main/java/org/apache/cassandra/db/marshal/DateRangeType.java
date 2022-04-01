/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRangeUtil;
import org.apache.cassandra.serializers.DateRangeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.solr.schema.DateRangeField;

/**
 * Date range C* type with lower and upper bounds represented as timestamps with a millisecond
 * precision. Backing C* type for Solr type {@link DateRangeField}. CQL input must be a valid {@link
 * DateRangeField} string.
 */
public class DateRangeType extends AbstractType<DateRange> {
  public static final DateRangeType instance = new DateRangeType();

  private DateRangeType() {
    super(ComparisonType.BYTE_ORDER);
  }

  @Override
  public ByteBuffer fromString(String source) throws MarshalException {
    if (source.isEmpty()) {
      return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }
    try {
      DateRange dateRange = DateRangeUtil.parseDateRange(source);
      return decompose(dateRange);
    } catch (Exception e) {
      throw new MarshalException(
          String.format("Could not parse date range: %s %s", source, e.getMessage()), e);
    }
  }

  @Override
  public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
    DateRange dateRange = this.getSerializer().deserialize(buffer);
    return '"' + dateRange.formatToSolrString() + '"';
  }

  @Override
  public Term fromJSONObject(Object parsed) throws MarshalException {
    if (parsed instanceof String) {
      return new Constants.Value(fromString((String) parsed));
    }
    throw new MarshalException(
        String.format(
            "Expected a string representation of a date range value, but got a %s: %s",
            parsed.getClass().getSimpleName(), parsed));
  }

  @Override
  public boolean isEmptyValueMeaningless() {
    return true;
  }

  @Override
  public TypeSerializer<DateRange> getSerializer() {
    return DateRangeSerializer.instance;
  }
}
