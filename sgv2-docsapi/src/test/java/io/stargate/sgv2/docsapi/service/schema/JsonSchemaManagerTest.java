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
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import io.stargate.sgv2.docsapi.service.schema.query.JsonSchemaQueryProvider;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
class JsonSchemaManagerTest extends BridgeTest {

  @Inject TableManager tableManager;

  @Inject JsonSchemaManager jsonSchemaManager;

  @Inject JsonSchemaQueryProvider queryProvider;

  @Inject ObjectMapper objectMapper;

  @Inject DocumentProperties documentProperties;

  @Inject GrpcClients grpcClients;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<QueryOuterClass.Query> queryCaptor;

  @BeforeEach
  public void init() {
    queryCaptor = ArgumentCaptor.forClass(QueryOuterClass.Query.class);
    doAnswer(invocation -> grpcClients.bridgeClient(Optional.empty(), Optional.empty()))
        .when(requestInfo)
        .getStargateBridge();
  }

  Schema.CqlKeyspaceDescribe getValidTableAndKeyspace(String namespace, String collection) {
    QueryOuterClass.ColumnSpec.Builder partitionColumn =
        QueryOuterClass.ColumnSpec.newBuilder()
            .setName(documentProperties.tableProperties().keyColumnName());
    Set<QueryOuterClass.ColumnSpec> clusteringColumns =
        documentProperties.tableColumns().pathColumnNames().stream()
            .map(c -> QueryOuterClass.ColumnSpec.newBuilder().setName(c).build())
            .collect(Collectors.toSet());
    Set<QueryOuterClass.ColumnSpec> valueColumns =
        documentProperties.tableColumns().valueColumnNames().stream()
            .map(c -> QueryOuterClass.ColumnSpec.newBuilder().setName(c).build())
            .collect(Collectors.toSet());

    Schema.CqlTable.Builder table =
        Schema.CqlTable.newBuilder()
            .setName(collection)
            .putOptions("comment", "{\"schema\": {}}")
            .addPartitionKeyColumns(partitionColumn)
            .addAllClusteringKeyColumns(clusteringColumns)
            .addAllColumns(valueColumns);
    return Schema.CqlKeyspaceDescribe.newBuilder()
        .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(namespace))
        .addTables(table)
        .build();
  }

  @Nested
  class AttachJsonSchema {
    @Test
    public void happyPath() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Response response = QueryOuterClass.Response.newBuilder().build();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
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
  }

  @Nested
  class GetJsonSchema {

    @Test
    public void happyPath() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      UniAssertSubscriber<JsonNode> result =
          jsonSchemaManager
              .getJsonSchema(tableManager, namespace, collection)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result
          .awaitItem()
          .assertItem(objectMapper.readTree(keyspace.getTables(0).getOptionsMap().get("comment")))
          .assertCompleted();

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }

  @Nested
  class ValidateJsonSchema {
    @Test
    public void happyPath() throws JsonProcessingException {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      Schema.CqlKeyspaceDescribe keyspace = getValidTableAndKeyspace(namespace, collection);

      doAnswer(
              invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer =
                    invocationOnMock.getArgument(1);
                observer.onNext(keyspace);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .describeKeyspace(any(), any());

      JsonNode document = objectMapper.readTree("{\"something\": \"json\"}");

      UniAssertSubscriber<Boolean> result =
          jsonSchemaManager
              .validateJsonSchema(tableManager, namespace, collection, document)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create());

      result.awaitItem().assertItem(true).assertCompleted();

      verify(bridgeService).describeKeyspace(any(), any());
      verifyNoMoreInteractions(bridgeService);
    }
  }
}
