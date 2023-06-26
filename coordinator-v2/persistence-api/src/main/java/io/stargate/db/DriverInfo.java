package io.stargate.db;

import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface DriverInfo {
  String name();

  Optional<String> version();

  static DriverInfo of(String name) {
    return of(name, null);
  }

  static DriverInfo of(String name, @Nullable String version) {
    return ImmutableDriverInfo.builder().name(name).version(Optional.ofNullable(version)).build();
  }
}
