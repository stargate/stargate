package io.stargate.db.schema;

import java.util.Collection;

/** An interface for calculating deterministic hash codes for schema objects. */
public interface SchemaHash {

  /**
   * Calculates a deterministic hash code for the schema object that does not change between
   * invocations of the JVM.
   *
   * @return a "stable" hash code for the schema object.
   */
  int schemaHashCode();

  static int hashCode(SchemaHash object) {
    if (object == null) {
      return 0;
    }
    return object.schemaHashCode();
  }

  static int enumHashCode(Enum<?> object) {
    if (object == null) {
      return 0;
    }
    // FIXME: Is this good enough of a hash?
    return object.ordinal() + 1; // Add one so that it's not ambiguous with null
  }

  static int combine(int... codes) {
    int result = 1;
    for (int code : codes) result = 31 * result + code;
    return result;
  }

  static int hash(Collection<? extends SchemaHash> collection) {
    return collection.stream()
        .map(o -> o == null ? 0 : o.schemaHashCode())
        .reduce(1, (r, h) -> 31 * r + h);
  }
}
