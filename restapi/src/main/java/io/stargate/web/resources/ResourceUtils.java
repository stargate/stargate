package io.stargate.web.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class ResourceUtils {

  public static Map<String, Object> readJson(String payload, ObjectMapper mapper) {
    Map<String, Object> requestBody;
    try {
      requestBody = mapper.readValue(payload, new TypeReference<HashMap<String, Object>>() {});
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Input provided is not valid json. %s", e.getMessage()));
    }
    return requestBody;
  }
}
