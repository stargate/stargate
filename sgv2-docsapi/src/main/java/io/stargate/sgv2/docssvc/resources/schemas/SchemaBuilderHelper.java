package io.stargate.sgv2.docssvc.resources.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class SchemaBuilderHelper {
  private static final int DEFAULT_REPLICAS_FOR_SIMPLE = 1;
  private static final int DEFAULT_REPLICAS_FOR_NETWORKED = 3;

  private final ObjectMapper jsonMapper;

  public SchemaBuilderHelper(ObjectMapper m) {
    jsonMapper = m;
  }

  public KeyspaceCreateDefinition readKeyspaceCreateDefinition(JsonNode payload) {
    // First simple check: needs to be JSON Object
    if (!payload.isObject()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid JSON payload for Keyspace creation; must be JSON Object: %s", payload));
    }

    // Then unlikely case of structural mismatch
    KeyspaceCreateDefinition kdef;
    try {
      kdef = jsonMapper.treeToValue(payload, KeyspaceCreateDefinition.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format(
              "Malformatted JSON payload (%s) for Keyspace creation, problem %s",
              payload, e.getMessage()));
    }

    // Must have name for keyspace to create
    if (kdef.name == null) {
      throw new IllegalArgumentException(
          String.format("Missing 'name' String property in payload Object: %s", payload));
    }

    // Some validation/augmentation now: verify required, add defaulting
    if (kdef.datacenters != null) {
      if (kdef.datacenters.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "malformatted JSON payload (%s) for Keyspace creation: empty 'datacenters'",
                payload));
      }
      // must have "name"s; will default replica counts as well
      for (Map.Entry<String, Integer> entry : kdef.datacenters.entrySet()) {
        String name = entry.getKey();
        if (name == null || name.isEmpty()) {
          throw new IllegalArgumentException(
              String.format(
                  "malformatted JSON payload (%s) for Keyspace creation: one of 'datacenters' entries missing 'name'",
                  payload));
        }
        if (entry.getValue() == null) {
          entry.setValue(DEFAULT_REPLICAS_FOR_NETWORKED);
        }
      }
    }
    return kdef;
  }

  /*
   Helper classes for databinding
  */

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class KeyspaceCreateDefinition {
    public String name;
    public int replicas = DEFAULT_REPLICAS_FOR_SIMPLE;
    public Map<String, Integer> datacenters;
  }
}
