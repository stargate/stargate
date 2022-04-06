package com.datastax.bdp.search.solr;

import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Cql3Utils {
  public static String typeToString(AbstractType type, Object value) {
    if (value.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)) {
      return "";
    }

    if (type instanceof ReversedType) {
      type = ((ReversedType) type).baseType;
    }

    if (type instanceof DateType) {
      if (value instanceof ByteBuffer) {
        value = ((DateType) type).compose((ByteBuffer) value);
      }
      return Long.toString(((Date) value).getTime());
    } else if (type instanceof TimestampType) {
      if (value instanceof ByteBuffer) {
        value = ((TimestampType) type).compose((ByteBuffer) value);
      }
      return Long.toString(((Date) value).getTime());
    } else if (type instanceof AbstractGeometricType) {
      OgcGeometry geometry = (OgcGeometry) type.getSerializer().deserialize((ByteBuffer) value);
      return geometry.asWellKnownText();
    } else {
      return value instanceof ByteBuffer ? type.getString((ByteBuffer) value) : value.toString();
    }
  }
}
