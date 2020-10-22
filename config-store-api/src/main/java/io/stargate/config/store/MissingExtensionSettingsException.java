package io.stargate.config.store;

/** Denotes that the underlying config-store does not have any setting for a given extension. */
public class MissingExtensionSettingsException extends RuntimeException {
  public MissingExtensionSettingsException(String message) {
    super(message);
  }
}
