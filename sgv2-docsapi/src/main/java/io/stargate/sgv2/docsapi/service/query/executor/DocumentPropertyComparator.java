package io.stargate.sgv2.docsapi.service.query.executor;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class DocumentPropertyComparator implements Comparator<DocumentProperty> {

  private final List<String> documentPathsColumns;

  public DocumentPropertyComparator(List<String> documentPathsColumns) {
    this.documentPathsColumns = documentPathsColumns;
  }

  /** {@inheritDoc} */
  @Override
  public int compare(DocumentProperty p1, DocumentProperty p2) {
    // always first compare by using COMPARABLE_BYTES_COMPARATOR
    int byteCompare = DocumentProperty.COMPARABLE_BYTES_COMPARATOR.compare(p1, p2);
    if (byteCompare != 0) {
      return byteCompare;
    }

    // if they are same, go for the path props
    int result = 0;
    for (String path : documentPathsColumns) {
      String key1 = p1.keyValue(path);
      String key2 = p2.keyValue(path);

      // if both keys are null, we can short-circuit here
      if (null == key1 && null == key2) {
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
