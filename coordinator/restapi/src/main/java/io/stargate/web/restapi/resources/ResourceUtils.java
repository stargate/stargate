package io.stargate.web.restapi.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.util.HashMap;
import java.util.Map;

public class ResourceUtils {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectReader MAP_READER =
      mapper.readerFor(
          mapper.getTypeFactory().constructType(new TypeReference<HashMap<String, Object>>() {}));

  public static Map<String, Object> readJson(String payload) {
    Map<String, Object> requestBody;
    try {
      requestBody = MAP_READER.readValue(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Input provided is not valid json. %s", e.getMessage()));
    }
    return requestBody;
  }
}
