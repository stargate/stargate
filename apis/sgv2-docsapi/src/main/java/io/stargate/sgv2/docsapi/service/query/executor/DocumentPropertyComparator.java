package io.stargate.sgv2.docsapi.service.query.executor;

import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class DocumentPropertyComparator implements Comparator<DocumentProperty> {

  /**
   * Comparator for comparing {@link DocumentProperty#comparableKey()}s. Compares bytes in unsigned
   * way.
   */
  public static final Comparator<ByteString> COMPARABLE_BYTES_COMPARATOR =
      Comparator.nullsLast(ByteString.unsignedLexicographicalComparator());

  private final List<String> documentPathsColumns;

  public DocumentPropertyComparator(List<String> documentPathsColumns) {
    this.documentPathsColumns = documentPathsColumns;
  }

  /** {@inheritDoc} */
  @Override
  public int compare(DocumentProperty p1, DocumentProperty p2) {
    // always first compare by using COMPARABLE_BYTES_COMPARATOR
    int byteCompare = COMPARABLE_BYTES_COMPARATOR.compare(p1.comparableKey(), p2.comparableKey());
    if (byteCompare != 0) {
      return byteCompare;
    }

    // if they are same, go for the path props
    int result = 0;
    for (String path : documentPathsColumns) {
      String key1 = p1.keyValue(path);
      String key2 = p2.keyValue(path);

      // if both keys are null or empty, we can short-circuit here
      if (StringUtils.isEmpty(key1) && StringUtils.isEmpty(key2)) {
        return 0;
      }

      // otherwise compare and return only if not equal
      result = Objects.compare(key1, key2, Comparator.naturalOrder());
      if (result != 0) {
        return result;
      }
    }
    return result;
  }
}
