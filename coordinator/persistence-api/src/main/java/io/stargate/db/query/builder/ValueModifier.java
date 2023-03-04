package io.stargate.db.query.builder;

import io.stargate.db.query.Modification.Operation;
import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Immutable
@Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class ValueModifier {

  abstract Target target();

  abstract Operation operation();

  abstract Value<?> value();

  public static ValueModifier set(String columnName, Object value) {
    return of(columnName, Value.of(value));
  }

  public static ValueModifier marker(String columnName) {
    return of(columnName, Value.marker());
  }

  public static ValueModifier of(String columnName, Value<?> value) {
    return ImmutableValueModifier.builder()
        .target(Target.of(columnName))
        .operation(Operation.SET)
        .value(value)
        .build();
  }

  public static ValueModifier of(String columnName, Value<?> value, Operation operation) {
    return ImmutableValueModifier.builder()
        .target(Target.of(columnName))
        .operation(operation)
        .value(value)
        .build();
  }

  @Immutable
  @Style(visibility = ImplementationVisibility.PACKAGE)
  abstract static class Target {

    abstract String columnName();

    abstract @Nullable String fieldName(); // only set for UDT field access

    abstract @Nullable Value<?> mapKey(); // only set for map value access

    static Target of(String columnName) {
      return ImmutableTarget.builder().columnName(columnName).build();
    }
  }
}
