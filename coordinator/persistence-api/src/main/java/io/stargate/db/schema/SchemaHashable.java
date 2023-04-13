package io.stargate.db.schema;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** An interface for calculating deterministic hash codes for schema objects. */
public interface SchemaHashable {

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

  static int hashCode(SchemaHashable object) {
    if (object == null) {
      return -1;
    }
    return object.schemaHashCode();
  }

  // 12-Apr-2023, tatu: This is still dangerous; should get rid of it. But reduce usage for now
  static int hashCode(Object object) {
    // It's still possible that something like `Optional<Enum>` would make it through. So that's
    // something to look out for.
    if (object instanceof Enum
        || object instanceof SchemaHashable
        || object instanceof Collection) {
      // Ideally wouldn't be called on Map either but there is one such usage... alas.
      throw new IllegalArgumentException(
          "Using `SchemaHash#hashCode(Object object)` on this object type (`Collection`, `Enum`, `SchemaHash`) may produce an non-deterministic hash code");
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
      return -1;
    }
    return object.name().hashCode();
  }

  static int hashCode(Map<String, String> map) {
    return Objects.hashCode(map);
  }

  static int hashCode(String str) {
    return (str == null) ? -1 : str.hashCode();
  }

  static int combine(int... hashCodes) {
    int result = 1;
    for (int code : hashCodes) result = 31 * result + code;
    return result;
  }

  static int hash(Collection<? extends SchemaHashable> collection) {
    return collection.stream()
        .map(o -> o == null ? 0 : o.schemaHashCode())
        .reduce(1, (r, h) -> 31 * r + h);
  }
}
