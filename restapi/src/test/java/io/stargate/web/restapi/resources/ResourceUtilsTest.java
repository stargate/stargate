package io.stargate.web.restapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ResourceUtilsTest {

  @Nested
  class ReadJson {

    @Test
    public void readValidJson() {
      String payload = "{\"a\": \"alpha\", \"b\": 1}";
      Map<String, Object> requestBodyMap = ResourceUtils.readJson(payload);

      assertThat(requestBodyMap.get("a")).isEqualTo("alpha");
      assertThat(requestBodyMap.get("b")).isEqualTo(1);
    }

    @Test
    public void readInvalidJsonMissingColon() {
      String payload = "{\"a\": \"alpha\", \"b\" 1}";
      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> ResourceUtils.readJson(payload));
      assertThat(ex)
          .hasMessage(
              "Input provided is not valid json. Unexpected character ('1' (code 49)): was expecting a colon to separate field name and value\n"
                  + " at [Source: (String)\"{\"a\": \"alpha\", \"b\" 1}\"; line: 1, column: 21]");
    }

    @Test
    public void readInvalidJsonDoubleQuote() {
      String payload = "{\"a\": \"\"alpha\", \"b\": 1}";
      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> ResourceUtils.readJson(payload));
      assertThat(ex)
          .hasMessage(
              "Input provided is not valid json. Unexpected character ('a' (code 97)): was expecting comma to separate Object entries\n"
                  + " at [Source: (String)\"{\"a\": \"\"alpha\", \"b\": 1}\"; line: 1, column: 10]");
    }
  }
}
