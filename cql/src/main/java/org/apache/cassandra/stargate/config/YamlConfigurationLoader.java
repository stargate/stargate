package org.apache.cassandra.stargate.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.stargate.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class YamlConfigurationLoader {

  private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

  public Config loadConfig(URL url) throws ConfigurationException {
    try {
      logger.debug("Loading settings from {}", url);
      byte[] configBytes;
      try (InputStream is = url.openStream()) {
        configBytes = ByteStreams.toByteArray(is);
      } catch (IOException e) {
        throw new ConfigurationException(
            "Unable to open yaml file: "
                + url
                + System.lineSeparator()
                + " Error: "
                + e.getMessage(),
            false);
      }

      Constructor constructor =
          new YamlConfigurationLoader.CustomConstructor(Config.class, Yaml.class.getClassLoader());
      YamlConfigurationLoader.PropertiesChecker propertiesChecker =
          new YamlConfigurationLoader.PropertiesChecker();
      constructor.setPropertyUtils(propertiesChecker);
      Yaml yaml = new Yaml(constructor);
      Config result = loadConfig(yaml, configBytes);
      propertiesChecker.check();
      return result;
    } catch (YAMLException e) {
      throw new ConfigurationException(
          "Invalid yaml: " + url + System.lineSeparator() + " Error: " + e.getMessage(), false);
    }
  }

  static class CustomConstructor extends CustomClassLoaderConstructor {

    CustomConstructor(Class<?> theRoot, ClassLoader classLoader) {
      super(theRoot, classLoader);
    }

    @Override
    protected List<Object> createDefaultList(int initSize) {
      return Lists.newCopyOnWriteArrayList();
    }

    @Override
    protected Map<Object, Object> createDefaultMap(int initSize) {
      return Maps.newConcurrentMap();
    }

    @Override
    protected Set<Object> createDefaultSet(int initSize) {
      return Sets.newConcurrentHashSet();
    }
  }

  private static Config loadConfig(Yaml yaml, byte[] configBytes) {
    Config config = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
    // If the configuration file is empty yaml will return null. In this case we should use the
    // default
    // configuration to avoid hitting a NPE at a later stage.
    return config == null ? new Config() : config;
  }

  /**
   * Utility class to check that there are no extra properties and that properties that are not null
   * by default are not set to null.
   */
  private static class PropertiesChecker extends PropertyUtils {

    private final Set<String> missingProperties = new HashSet<>();

    private final Set<String> nullProperties = new HashSet<>();

    public PropertiesChecker() {
      setSkipMissingProperties(true);
    }

    @Override
    public Property getProperty(Class<? extends Object> type, String name) {
      final Property result = super.getProperty(type, name);

      if (result instanceof MissingProperty) {
        missingProperties.add(result.getName());
      }

      return new Property(result.getName(), result.getType()) {
        @Override
        public void set(Object object, Object value) throws Exception {
          if (value == null && get(object) != null) {
            nullProperties.add(getName());
          }
          result.set(object, value);
        }

        @Override
        public Class<?>[] getActualTypeArguments() {
          return result.getActualTypeArguments();
        }

        @Override
        public Object get(Object object) {
          return result.get(object);
        }

        @Override
        public List<Annotation> getAnnotations() {
          return Collections.EMPTY_LIST;
        }

        @Override
        public <A extends Annotation> A getAnnotation(Class<A> aClass) {
          return null;
        }
      };
    }

    public void check() throws ConfigurationException {
      if (!nullProperties.isEmpty()) {
        throw new ConfigurationException(
            "Invalid yaml. Those properties " + nullProperties + " are not valid", false);
      }

      if (!missingProperties.isEmpty()) {
        throw new ConfigurationException(
            "Invalid yaml. Please remove properties "
                + missingProperties
                + " from your cassandra.yaml",
            false);
      }
    }
  }
}
