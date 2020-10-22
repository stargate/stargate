package io.stargate.config.store.k8s.config.map;

import java.io.IOException;

public class YamlConfigException extends RuntimeException {

  public YamlConfigException(String message, IOException e) {
    super(message, e);
  }
}
