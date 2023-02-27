package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import io.stargate.web.docsapi.dao.DocumentDB;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JsonSchemaHandlerTest {
  private JsonSchemaHandler schemaHandler;
  private static final ObjectMapper mapper = new ObjectMapper();
  private JsonNode schema;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private DocumentDB dbMock;

  @BeforeEach
  void setup() throws JsonProcessingException {
    schemaHandler = new JsonSchemaHandler(mapper);
    schema =
        mapper.readTree(
            "{\n"
                + "    \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
                + "    \"title\": \"Product\",\n"
                + "    \"description\": \"A product from the catalog\",\n"
                + "    \"type\": \"object\",\n"
                + "    \"properties\": {\n"
                + "      \"id\": {\n"
                + "        \"description\": \"The unique identifier for a product\",\n"
                + "        \"type\": \"integer\"\n"
                + "      },\n"
                + "      \"name\": {\n"
                + "        \"description\": \"Name of the product\",\n"
                + "        \"type\": \"string\"\n"
                + "      },\n"
                + "      \"price\": {\n"
                + "        \"type\": \"number\",\n"
                + "        \"minimum\": 0,\n"
                + "        \"exclusiveMinimum\": true\n"
                + "      }\n"
                + "    },\n"
                + "    \"required\": [\"id\", \"name\", \"price\"]\n"
                + "  }");
  }

  @Test
  void testValidate() throws JsonProcessingException, ProcessingException {
    schemaHandler.validate(schema, "{\"id\":1,\"name\":\"a\",\"price\":1}");

    ThrowableAssert.ThrowingCallable action =
        () -> schemaHandler.validate(schema, "{\"id\":1,\"price\":1}");
    assertThatThrownBy(action)
        .hasMessage("Invalid JSON: [object has missing required properties ([\"name\"])]");

    action = () -> schemaHandler.validate(schema, "{\"id\":1,\"name\":\"a\",\"price\":-1}");
    assertThatThrownBy(action)
        .hasMessage(
            "Invalid JSON: [numeric instance is lower than the required minimum (minimum: 0, found: -1)]");

    action = () -> schemaHandler.validate(schema, "{}");
    assertThatThrownBy(action)
        .hasMessage(
            "Invalid JSON: [object has missing required properties ([\"id\",\"name\",\"price\"])]");
  }
}
