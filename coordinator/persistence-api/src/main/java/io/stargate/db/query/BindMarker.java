package io.stargate.db.query;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.util.Objects;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
public interface BindMarker {
  String receiver();

  ColumnType type();

  static BindMarker markerFor(String receiver, ColumnType type) {
    return ImmutableBindMarker.builder().receiver(receiver).type(type).build();
  }

  static BindMarker markerFor(Column column) {
    Objects.requireNonNull(column.name(), "The column for a bind marker must have a name");
    Objects.requireNonNull(column.type(), "The column for a bind marker must have a type");
    return markerFor(column.name(), column.type());
  }
}
