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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.docsapi.service;

import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Regular;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.Builder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.TimeSource;
import io.stargate.db.BatchType;
import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDBFactory;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.ImmutableExecutionProfile;
import io.stargate.web.docsapi.models.MultiDocsResponse;
import io.stargate.web.docsapi.models.QueryInfo;
import io.stargate.web.docsapi.resources.DocumentResourceV2;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DocumentServiceTest extends AbstractDataStoreTest {

  private static final Object SEPARATOR = new Object();

  private final String insert =
      "INSERT INTO test_docs.collection1 (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

  protected static final Table table =
      ImmutableTable.builder()
          .keyspace("test_docs")
          .name("collection1")
          .addColumns(
              ImmutableColumn.builder().name("key").type(Type.Text).kind(PartitionKey).build())
          .addColumns(clusteringColumns())
          .addColumns(ImmutableColumn.builder().name("leaf").type(Type.Text).kind(Regular).build())
          .addColumns(
              ImmutableColumn.builder().name("text_value").type(Type.Text).kind(Regular).build())
          .addColumns(
              ImmutableColumn.builder().name("dbl_value").type(Type.Double).kind(Regular).build())
          .addColumns(
              ImmutableColumn.builder().name("bool_value").type(Type.Boolean).kind(Regular).build())
          .build();

  private static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("test_docs").addTables(table).build();

  private static final Schema schema = ImmutableSchema.builder().addKeyspaces(keyspace).build();
  private static final ObjectMapper mapper = new ObjectMapper();

  @Mock private TimeSource timeSource;
  private static final DocsApiConfiguration config = DocsApiConfiguration.DEFAULT;
  private final DocsSchemaChecker schemaChecker = new DocsSchemaChecker();
  private final String authToken = "test-auth-token";
  private final AuthenticationSubject subject = AuthenticationSubject.of(authToken, "user1", false);
  @Mock private JsonSchemaHandler jsonSchemaHandler;
  private final DocsShredder docsShredder = new DocsShredder(config);
  @Mock private AuthenticationService authenticationService;
  @Mock private AuthorizationService authorizationService;
  @Mock private DataStoreFactory dataStoreFactory;
  @Mock private UriInfo uriInfo;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private HttpHeaders headers;

  @Mock(answer = RETURNS_DEEP_STUBS)
  private HttpServletRequest request;

  private DocumentDBFactory documentDBFactory;
  private DocumentService service;
  private DocumentResourceV2 resource;

  private static Column[] clusteringColumns() {
    Column[] columns = new Column[64];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = ImmutableColumn.builder().name("p" + i).type(Type.Text).kind(Clustering).build();
    }
    return columns;
  }

  @Override
  protected Schema schema() {
    return schema;
  }

  @BeforeAll
  public static void setupJackson() {
    mapper.registerModule(new GuavaModule());
  }

  @BeforeEach
  void setup() throws UnauthorizedException {
    when(dataStoreFactory.create(any(), any())).thenReturn(datastore());

    documentDBFactory =
        new DocumentDBFactory(
            authenticationService, authorizationService, dataStoreFactory, config);

    when(authenticationService.validateToken(eq(authToken), anyMap())).thenReturn(subject);
    service =
        new DocumentService(
            timeSource, mapper, schemaChecker, jsonSchemaHandler, docsShredder, config);
    resource = new DocumentResourceV2(documentDBFactory, mapper, service, config, schemaChecker);
  }

  private Map<String, Object> leafRow(String id) {
    return m("key", id, "leaf", "test-value");
  }

  private Map<String, Object> row(String id, Double value, String... path) {
    return row(id, 123, value, path);
  }

  private Map<String, Object> row(String id, int timestamp, Double value, String... path) {
    Builder<String, Object> map = row(id, timestamp, path);
    map.put("dbl_value", value);
    return map.build();
  }

  private Builder<String, Object> row(String id, int timestamp, String... path) {
    Builder<String, Object> map = ImmutableMap.builder();
    map.put("key", id);
    map.put("writetime(leaf)", timestamp);
    String leaf = null;
    for (int i = 0; i < config.getMaxDepth(); i++) {
      String p;
      if (i < path.length) {
        p = path[i];
        leaf = p;
      } else {
        p = "";
      }

      map.put("p" + i, p);
    }

    assertThat(leaf).isNotNull();
    map.put("leaf", leaf);
    return map;
  }

  private <T> DocumentResponseWrapper<T> unwrap(Response r) throws JsonProcessingException {
    assertThat(r.getStatus())
        .withFailMessage("Unexpected error code: " + r.getStatus() + ", message: " + r.getEntity())
        .isEqualTo(Status.OK.getStatusCode());

    String entity = (String) r.getEntity();
    @SuppressWarnings("unchecked")
    DocumentResponseWrapper<T> resp = mapper.readValue(entity, DocumentResponseWrapper.class);

    assertThat(resp).isNotNull();
    return resp;
  }

  private PathSegment p(String segment) {
    return new PathSegment() {
      @Override
      public String getPath() {
        return segment;
      }

      @Override
      public MultivaluedMap<String, String> getMatrixParameters() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Map<String, Object> m(Object... keyValues) {
    assertThat(keyValues.length).isEven();
    Builder<String, Object> map = ImmutableMap.builder();
    for (int i = 0; i < keyValues.length; i++) {
      Object key = keyValues[i++];
      Object value = keyValues[i];
      map.put(key.toString(), value);
    }
    return map.build();
  }

  private String selectAll(String where) {
    return "SELECT key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM test_docs.collection1"
        + (where.isEmpty() ? "" : " " + where);
  }

  @Test
  private Object[] params(Object... params) {
    return params;
  }

  private Object[] fillParams(int totalCount, Object... params) {
    Object[] result = new Object[totalCount];
    Arrays.fill(result, ""); // default value for pNN columns
    int idx = 0;
    for (Object param : params) {
      if (param == SEPARATOR) {
        idx = totalCount - params.length + idx + 1;
        continue;
      }

      result[idx++] = param;
    }

    return result;
  }

  @Test
  void testPutAtPathRoot() throws UnauthorizedException, ProcessingException {
    BatchType batchType =
        datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

    withQuery(table, "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?", 99L, "id1")
        .inBatch(batchType)
        .returningNothing();

    withQuery(table, insert, fillParams(70, "id1", "a", SEPARATOR, "a", null, 123.0d, null, 100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(table, insert, fillParams(70, "id1", "b", SEPARATOR, "b", null, null, true, 100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(table, insert, fillParams(70, "id1", "c", SEPARATOR, "c", "text", null, null, 100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(
            table,
            insert,
            fillParams(
                70,
                "id1",
                "d",
                SEPARATOR,
                "d",
                DocsApiConstants.EMPTY_OBJECT_MARKER,
                null,
                null,
                100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(
            table,
            insert,
            fillParams(
                70,
                "id1",
                "e",
                SEPARATOR,
                "e",
                DocsApiConstants.EMPTY_ARRAY_MARKER,
                null,
                null,
                100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(table, insert, fillParams(70, "id1", "f", SEPARATOR, "f", null, null, null, 100L))
        .inBatch(batchType)
        .returningNothing();
    withQuery(
            table,
            insert,
            fillParams(70, "id1", "g", "[000000]", "h", SEPARATOR, "h", null, 1.0d, null, 100L))
        .inBatch(batchType)
        .returningNothing();

    when(timeSource.currentTimeMicros()).thenReturn(100L);
    service.putAtPath(
        authToken,
        keyspace.name(),
        table.name(),
        "id1",
        "{\"a\":123, \"b\":true, \"c\":\"text\", \"d\":{}, \"e\":[], \"f\":null, \"g\":[{\"h\":1}]}",
        ImmutableList.of(),
        false,
        documentDBFactory,
        true,
        Collections.emptyMap(),
        ExecutionContext.NOOP_CONTEXT);
  }

  @Test
  void testPutAtPathNested() throws UnauthorizedException, ProcessingException {
    BatchType batchType =
        datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

    withQuery(
            table,
            "DELETE FROM test_docs.collection1 USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ?",
            199L,
            "id2",
            "x",
            "y",
            "[000000]")
        .inBatch(batchType)
        .returningNothing();

    String insert =
        "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    withQuery(
            table,
            insert,
            fillParams(
                70, "id2", "x", "y", "[000000]", "a", SEPARATOR, "a", null, 123.0d, null, 200L))
        .inBatch(batchType)
        .returningNothing();

    when(timeSource.currentTimeMicros()).thenReturn(200L);
    service.putAtPath(
        authToken,
        keyspace.name(),
        table.name(),
        "id2",
        "{\"a\":123}",
        ImmutableList.of(p("x"), p("y"), p("[000000]")),
        false,
        documentDBFactory,
        true,
        Collections.emptyMap(),
        ExecutionContext.NOOP_CONTEXT);
  }

  @Test
  void testPutAtPathPatch() throws UnauthorizedException, ProcessingException {
    BatchType batchType =
        datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

    String insert =
        "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    withQuery(table, insert, fillParams(70, "id3", "a", SEPARATOR, "a", null, 123.0d, null, 200L))
        .inBatch(batchType)
        .returningNothing();

    withQuery(
            table,
            "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?",
            199L,
            "id3",
            "[000000]",
            "[999999]")
        .inBatch(batchType)
        .returningNothing();
    withQuery(
            table,
            "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 IN ?",
            199L,
            "id3",
            ImmutableList.of("a"))
        .inBatch(batchType)
        .returningNothing();

    when(timeSource.currentTimeMicros()).thenReturn(200L);
    service.putAtPath(
        authToken,
        keyspace.name(),
        table.name(),
        "id3",
        "{\"a\":123}",
        ImmutableList.of(),
        true,
        documentDBFactory,
        true,
        Collections.emptyMap(),
        ExecutionContext.NOOP_CONTEXT);
  }

  @Test
  void testPutAtPathUnauthorized() throws UnauthorizedException {
    ThrowingCallable action =
        () ->
            service.putAtPath(
                authToken,
                keyspace.name(),
                table.name(),
                "id3",
                "{\"a\":123}",
                ImmutableList.of(),
                true,
                documentDBFactory,
                true,
                Collections.emptyMap(),
                ExecutionContext.NOOP_CONTEXT);

    Mockito.doThrow(new UnauthorizedException("test1"))
        .when(authorizationService)
        .authorizeDataWrite(
            any(), eq(keyspace.name()), eq(table.name()), eq(Scope.DELETE), eq(SourceAPI.REST));

    assertThatThrownBy(action).hasMessage("test1");

    Mockito.doNothing()
        .when(authorizationService)
        .authorizeDataWrite(
            any(), eq(keyspace.name()), eq(table.name()), eq(Scope.DELETE), eq(SourceAPI.REST));
    Mockito.doThrow(new UnauthorizedException("test2"))
        .when(authorizationService)
        .authorizeDataWrite(
            any(), eq(keyspace.name()), eq(table.name()), eq(Scope.MODIFY), eq(SourceAPI.REST));

    assertThatThrownBy(action).hasMessage("test2");
  }

  @Test
  void testWriteManyDocs() throws UnauthorizedException, IOException {
    BatchType batchType =
        datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;
    ByteArrayInputStream in =
        new ByteArrayInputStream("[{\"a\":\"b\"}]".getBytes(StandardCharsets.UTF_8));
    when(timeSource.currentTimeMicros()).thenReturn(200L);
    withQuery(table, "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?", 199L, "b")
        .inBatch(batchType)
        .returningNothing();
    withQuery(table, insert, fillParams(70, "b", "a", SEPARATOR, "a", "b", null, null, 200L))
        .inBatch(batchType)
        .returningNothing();
    service.writeManyDocs(
        authToken,
        keyspace.name(),
        table.name(),
        in,
        Optional.of("a"),
        documentDBFactory,
        ExecutionContext.NOOP_CONTEXT,
        Collections.emptyMap());
  }

  @Test
  void testWriteManyDocs_invalidIdPath() {
    ByteArrayInputStream in =
        new ByteArrayInputStream("[{\"a\":\"b\"}]".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(
            () ->
                service.writeManyDocs(
                    authToken,
                    keyspace.name(),
                    table.name(),
                    in,
                    Optional.of("no.good"),
                    documentDBFactory,
                    ExecutionContext.NOOP_CONTEXT,
                    Collections.emptyMap()))
        .hasMessage(
            "Json Document {\"a\":\"b\"} requires a String value at the path no.good, found . Batch write failed.");
  }

  @Nested
  class Profiling {

    private final String dblValueGtQuery =
        "SELECT key, leaf FROM test_docs.collection1 WHERE p0 = ? AND p1 = ? AND p2 = ? AND dbl_value > ? ALLOW FILTERING";
    private final String dblValueEqQuery =
        "SELECT key, leaf FROM test_docs.collection1 WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ? AND dbl_value = ? LIMIT ? ALLOW FILTERING";
    private final String selectByKey = selectAll("WHERE key = ?");
    private final String selectAll = selectAll("");
    private final String id1 = "id1";
    private final String id2 = "id2";
    private final String id3 = "id3";

    @BeforeEach
    void setQueryExpectations() {
      BatchType batchType =
          datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

      withQuery(table, dblValueGtQuery, params("a", "c", "", 1.0))
          .returning(ImmutableList.of(leafRow(id1), leafRow(id2), leafRow(id3)));

      withQuery(table, dblValueEqQuery, id1, "d", "e", "f", "", 2.0, 1)
          .returning(ImmutableList.of(leafRow(id1)));
      withQuery(table, dblValueEqQuery, id2, "d", "e", "f", "", 2.0, 1)
          .returning(ImmutableList.of(leafRow(id2)));
      withQuery(table, dblValueEqQuery, id3, "d", "e", "f", "", 2.0, 1).returningNothing();

      withQuery(table, selectByKey, id1)
          .returning(ImmutableList.of(row(id1, 3.0, "a", "b"), row(id1, 4.0, "a", "c")));

      withQuery(table, selectByKey, id2)
          .returning(
              ImmutableList.of(
                  row(id2, 5.0, "a", "b"), row(id2, 10.0, "a", "c"), row(id2, 5.0, "a", "d")));

      withQuery(table, selectAll)
          .returning(
              ImmutableList.of(
                  row(id2, 5.0, "a", "b"), row(id2, 10.0, "a", "c"), row(id2, 5.0, "a", "d")));

      withQuery(table, insert, fillParams(70, id3, "a", SEPARATOR, "a", null, 123.0d, null, 200L))
          .inBatch(batchType)
          .returningNothing();
    }

    @AfterEach
    void ignoreExpectation() {
      // Tests in this class are meant to validate only profile output. We have some extra
      // expectations defined by setQueryExpectations() to reduce copy-paste, but we do not actually
      // expect all of them to be executed by each test method.
      resetExpectations();
    }

    @Test
    void putManyDocs() throws JsonProcessingException {
      BatchType batchType =
          datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

      String delete = "DELETE FROM test_docs.collection1 USING TIMESTAMP ? WHERE key = ?";
      withQuery(table, delete, 199L, "123").inBatch(batchType).returningNothing();
      withQuery(table, delete, 199L, "234").inBatch(batchType).returningNothing();
      withQuery(table, insert, fillParams(70, "123", "a", SEPARATOR, "a", "123", null, null, 200L))
          .inBatch(batchType)
          .returningNothing();
      withQuery(table, insert, fillParams(70, "234", "a", SEPARATOR, "a", "234", null, null, 200L))
          .inBatch(batchType)
          .returningNothing();

      when(timeSource.currentTimeMicros()).thenReturn(200L);
      Response r =
          resource.writeManyDocs(
              headers,
              uriInfo,
              authToken,
              keyspace.name(),
              table.name(),
              new ByteArrayInputStream("[{\"a\":\"123\"},{\"a\":\"234\"}]".getBytes()),
              "a",
              true,
              request);
      MultiDocsResponse mdr = mapper.readValue((String) r.getEntity(), MultiDocsResponse.class);
      assertThat(mdr.getProfile())
          .isEqualTo(
              ImmutableExecutionProfile.builder()
                  .description("root")
                  .addNested(
                      ImmutableExecutionProfile.builder()
                          .description("ASYNC INSERT")
                          // row count for DELETE is not known
                          .addQueries(QueryInfo.of(insert, 2, 2), QueryInfo.of(delete, 2, 0))
                          .build())
                  .build());
    }

    @Test
    void patchDoc() throws JsonProcessingException {
      BatchType batchType =
          datastore().supportsLoggedBatches() ? BatchType.LOGGED : BatchType.UNLOGGED;

      when(headers.getHeaderString(eq(HttpHeaders.CONTENT_TYPE))).thenReturn("application/json");

      String delete1 =
          "DELETE FROM test_docs.collection1 USING TIMESTAMP ? WHERE key = ? AND p0 >= ? AND p0 <= ?";
      withQuery(table, delete1, 199L, "id3", "[000000]", "[999999]")
          .inBatch(batchType)
          .returningNothing();
      String delete2 =
          "DELETE FROM test_docs.collection1 USING TIMESTAMP ? WHERE key = ? AND p0 IN ?";
      withQuery(table, delete2, 199L, "id3", ImmutableList.of("a"))
          .inBatch(batchType)
          .returningNothing();

      when(timeSource.currentTimeMicros()).thenReturn(200L);
      DocumentResponseWrapper<Object> r =
          unwrap(
              resource.patchDoc(
                  headers,
                  uriInfo,
                  authToken,
                  keyspace.name(),
                  table.name(),
                  "id3",
                  "{\"a\":123}",
                  true,
                  request));
      assertThat(r.getProfile())
          .isEqualTo(
              ImmutableExecutionProfile.builder()
                  .description("root")
                  .addNested(
                      ImmutableExecutionProfile.builder()
                          .description("ASYNC PATCH")
                          // row count for DELETE is not known
                          .addQueries(
                              QueryInfo.of(insert, 1, 1),
                              QueryInfo.of(delete2, 1, 0),
                              QueryInfo.of(delete1, 1, 0))
                          .build())
                  .build());
    }
  }
}
