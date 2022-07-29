package io.stargate.db.schema;

import java.util.Collection;
import java.util.Objects;

/** An interface for calculating deterministic hash codes for schema objects. */
public interface SchemaHash {

  /**
   * Calculates a deterministic hash code for the schema object that does not change between
   * invocations of the JVM. This was mostly because it was determined that hash codes of enums
   * could change between different executions of the JVM and schema hash codes need to be
   * deterministic across JVM instances because these codes are distributed to Stargate bridge
   * clients.
   *
   * <p>When calculating the schema hash code it advised that one of the `SchemaHash#hashCode()`
   * methods in this interface be used instead of using `obj.hashCode()`, {@link Object#hashCode()},
   * or {@link System#identityHashCode(Object)}, etc. to avoid an error that produces a
   * non-deterministic hash code.
   *
   * @return a deterministic hash code for the schema object.
   */
  int schemaHashCode();

  static int hashCode(SchemaHash object) {
    if (object == null) {
      return 0;
    }
    return object.schemaHashCode();
  }

  static int hashCode(Object object) {
    // It's still possible that something like `Optional<Enum>` would make it through. So that's
    // something to look out for.
    if (object instanceof Enum || object instanceof SchemaHash) {
      throw new AssertionError(
          "Using `SchemaHash#hashCode(Object object)` on this object type (`Enum`, `SchemaHash`) may produce an non-deterministic hash code");
    }
    return Objects.hashCode(object);
  }

  static int hashCode(int i) {
    return Integer.hashCode(i);
  }

  static int hashCode(boolean b) {
    return Boolean.hashCode(b);
  }

  static int enumHashCode(Enum<?> object) {
    if (object == null) {
      return 0;
    }
    // FIXME: Is this good enough of a hash?
    return object.ordinal() + 1; // Add one so that it's not ambiguous with null
  }

  static int combine(int... hashCodes) {
    int result = 1;
    for (int code : hashCodes) result = 31 * result + code;
    return result;
  }

  static int hash(Collection<? extends SchemaHash> collection) {
    return collection.stream()
        .map(o -> o == null ? 0 : o.schemaHashCode())
        .reduce(1, (r, h) -> 31 * r + h);
  }
}
