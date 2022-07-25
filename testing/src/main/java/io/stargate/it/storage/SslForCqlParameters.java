package io.stargate.it.storage;

import org.immutables.value.Value;

/** Parameters for configuring TLS for CQL native protocol. */
@Value.Immutable(prehash = true)
public interface SslForCqlParameters {

  @Value.Default
  default boolean enabled() {
    return false;
  }

  @Value.Default
  default boolean optional() {
    return false;
  }

  @Value.Default
  default boolean requireClientCertificates() {
    return false;
  }

  static Builder builder() {
    return ImmutableSslForCqlParameters.builder();
  }

  interface Builder {
    Builder enabled(boolean value);

    Builder optional(boolean value);

    Builder requireClientCertificates(boolean value);

    SslForCqlParameters build();
  }
}
