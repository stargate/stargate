package io.stargate.sgv2.docsapi.service.schema;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class JsonSchemaManagerTest extends BridgeTest {
  @Inject JsonSchemaManager jsonSchemaManager;

  @Inject ObjectMapper objectMapper;

  @Inject GrpcClients grpcClients;

  @InjectMock StargateRequestInfo requestInfo;

  Schema.CqlTable table;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  @BeforeEach
  public void init() {
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    doAnswer(invocation -> grpcClients.bridgeClient(Optional.empty(), Optional.empty()))
        .when(requestInfo)
        .getStargateBridge();
  }

  @Nested
  class AttachJsonSchema {
    @Test
    public void happyPath() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Query> observer = invocationOnMock.getArgument(1);
                observer.onNext(QueryOuterClass.Query.getDefaultInstance());
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      JsonNode schema =
          objectMapper.readTree("{\"$schema\": \"https://json-schema.org/draft/2019-09/schema\"}");

      jsonSchemaManager
          .attachJsonSchema(namespace, collection, schema)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      verify(bridgeService).executeQuery(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void malformedSchema() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      doAnswer(invocationOnMock -> null).when(bridgeService).executeQuery(any(), any());

      JsonNode schema =
          objectMapper.readTree(
              "{\n"
                  + "  \"$schema\": \"https://json-schema.org/draft/2019-09/schema\",\n"
                  + "  \"type\": \"object\",\n"
                  + "  \"properties\": {\n"
                  + "    \"something\": { \"type\": \"strin\" }\n"
                  + "  }\n"
                  + "}");

      jsonSchemaManager
          .attachJsonSchema(namespace, collection, schema)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure()
          .assertFailedWith(ErrorCodeRuntimeException.class);

      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class GetJsonSchema {

    @Test
    public void happyPath() throws JsonProcessingException {
      table = Schema.CqlTable.newBuilder().putOptions("comment", "{\"schema\": {}}").build();

      UniAssertSubscriber<JsonNode> result =
          jsonSchemaManager
              .getJsonSchema(Uni.createFrom().item(table))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(objectMapper.readTree("{\"schema\": {}}")).assertCompleted();
    }

    @Test
    public void noSchema() {
      table = Schema.CqlTable.newBuilder().build();

      UniAssertSubscriber<JsonNode> result =
          jsonSchemaManager
              .getJsonSchema(Uni.createFrom().item(table))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(null).assertCompleted();
    }

    @Test
    public void malformedJsonSchema() {
      table = Schema.CqlTable.newBuilder().putOptions("comment", "lorem ipsum").build();

      UniAssertSubscriber<JsonNode> result =
          jsonSchemaManager
              .getJsonSchema(Uni.createFrom().item(table))
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(null).assertCompleted();
    }
  }

  @Nested
  class ValidateJsonSchema {
    @Test
    public void happyPath() throws JsonProcessingException {
      table = Schema.CqlTable.newBuilder().putOptions("comment", testJsonSchema()).build();

      JsonNode document = objectMapper.readTree("{\"id\":1, \"name\": \"Eric\", \"price\":1}");

      UniAssertSubscriber<Boolean> result =
          jsonSchemaManager
              .validateJsonDocument(Uni.createFrom().item(table), document)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(true).assertCompleted();
    }

    @Test
    public void noSchemaAvailable() throws JsonProcessingException {
      table = Schema.CqlTable.newBuilder().build();

      JsonNode document = objectMapper.readTree("{\"something\": \"json\"}");

      UniAssertSubscriber<Boolean> result =
          jsonSchemaManager
              .validateJsonDocument(Uni.createFrom().item(table), document)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(true).assertCompleted();
    }
  }

  @Test
  public void documentSchemaMismatch() throws JsonProcessingException {
    table = Schema.CqlTable.newBuilder().putOptions("comment", testJsonSchema()).build();

    JsonNode document = objectMapper.readTree("{\"id\":1, \"price\":1}");
    jsonSchemaManager
        .validateJsonDocument(Uni.createFrom().item(table), document)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(ErrorCodeRuntimeException.class);
  }

  private String testJsonSchema() {
    return "{\"schema\": {\n"
        + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n"
        + "  \"title\": \"Product\",\n"
        + "  \"description\": \"A product from the catalog\",\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"id\": {\n"
        + "      \"description\": \"The unique identifier for a product\",\n"
        + "      \"type\": \"integer\"\n"
        + "    },\n"
        + "    \"name\": {\n"
        + "      \"description\": \"Name of the product\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    \"price\": {\n"
        + "      \"type\": \"number\",\n"
        + "      \"minimum\": 0,\n"
        + "      \"exclusiveMinimum\": true,\n"
        + "      \"description\": \"Product's price\"\n"
        + "    }\n"
        + "  },\n"
        + "  \"required\": [\"id\", \"name\", \"price\"]\n"
        + "}}";
  }
}
