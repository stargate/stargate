package io.stargate.config.store;

import java.io.Closeable;
import java.util.Map;

public interface ConfigStore extends Closeable {

  Map<String, String> getConfigForExtension(String extensionName);

}
