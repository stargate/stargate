package io.stargate.config.store;

import java.util.Map;

public interface ConfigStore {

  Map<String, Object> getConfigForExtension(String extensionName);
}
