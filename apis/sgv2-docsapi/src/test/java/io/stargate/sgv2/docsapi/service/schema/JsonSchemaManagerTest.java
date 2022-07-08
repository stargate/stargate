/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
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
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.GrpcClients;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
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
      table = Schema.CqlTable.newBuilder().setName(collection).putOptions("comment", "{}").build();

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
          .attachJsonSchema(namespace, Uni.createFrom().item(table), schema)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      verify(bridgeService).executeQuery(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }

    @Test
    public void malformedSchema() throws JsonProcessingException {
      table = Schema.CqlTable.newBuilder().build();
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      doAnswer(invocationOnMock -> null).when(bridgeService).executeQuery(any(), any());

      JsonNode schema =
          objectMapper.readTree(
              """
                  {
                    "$schema": "https://json-schema.org/draft/2019-09/schema",
                    "type": "object",
                    "properties": {
                      "something": { "type": "strin" }
                    }
                  }""");

      jsonSchemaManager
          .attachJsonSchema(namespace, Uni.createFrom().item(table), schema)
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

      result.awaitItem().assertItem(objectMapper.readTree("{}")).assertCompleted();
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
              .validateJsonDocument(Uni.createFrom().item(table), document, false)
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
              .validateJsonDocument(Uni.createFrom().item(table), document, false)
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
        .validateJsonDocument(Uni.createFrom().item(table), document, false)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(ErrorCodeRuntimeException.class);
  }

  @Test
  public void schemaPresentSubdocumentDisallowed() throws JsonProcessingException {
    table = Schema.CqlTable.newBuilder().putOptions("comment", testJsonSchema()).build();

    JsonNode document = objectMapper.readTree("{\"id\":1, \"name\": \"Eric\", \"price\":1}");
    Throwable failure =
        jsonSchemaManager
            .validateJsonDocument(Uni.createFrom().item(table), document, true)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(failure)
        .isInstanceOf(ErrorCodeRuntimeException.class)
        .hasFieldOrPropertyWithValue(
            "errorCode", ErrorCode.DOCS_API_JSON_SCHEMA_INVALID_PARTIAL_UPDATE);
  }

  private String testJsonSchema() {
    return """
            {"schema": {
              "$schema": "http://json-schema.org/draft-04/schema#",
              "title": "Product",
              "description": "A product from the catalog",
              "type": "object",
              "properties": {
                "id": {
                  "description": "The unique identifier for a product",
                  "type": "integer"
                },
                "name": {
                  "description": "Name of the product",
                  "type": "string"
                },
                "price": {
                  "type": "number",
                  "minimum": 0,
                  "exclusiveMinimum": true,
                  "description": "Product's price"
                }
              },
              "required": ["id", "name", "price"]
            }}""";
  }
}
