package io.stargate.web.docsapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.service.SchemalessService;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.resources.Db;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest
public class SchemalessResourceV2Test {
  private static final ObjectMapper mapper = new ObjectMapper();
  private SchemalessService schemalessServiceMock = mock(SchemalessService.class);
  private Db dbFactoryMock = mock(Db.class);
  private SchemalessResourceV2 schemalessResourceV2;
  private Method wrapResponseId;
  private Method wrapResponseIdPageState;

  @Before
  public void setup() throws NoSuchMethodException {
    schemalessResourceV2 = new SchemalessResourceV2();
    Whitebox.setInternalState(schemalessResourceV2, SchemalessService.class, schemalessServiceMock);
    Whitebox.setInternalState(schemalessResourceV2, Db.class, dbFactoryMock);

    wrapResponseId =
        SchemalessResourceV2.class.getDeclaredMethod("wrapResponse", JsonNode.class, String.class);
    wrapResponseId.setAccessible(true);
    wrapResponseIdPageState =
        SchemalessResourceV2.class.getDeclaredMethod(
            "wrapResponse", JsonNode.class, String.class, String.class);
    wrapResponseIdPageState.setAccessible(true);
  }

  @Test
  public void wrapResponse_nodeIdDefined()
      throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result = (JsonNode) wrapResponseId.invoke(schemalessResourceV2, node, "a");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf("a"));
    expected.set("data", node);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nodeDefined() throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result = (JsonNode) wrapResponseId.invoke(schemalessResourceV2, node, null);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("data", node);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_idDefined() throws IllegalAccessException, InvocationTargetException {
    JsonNode result = (JsonNode) wrapResponseId.invoke(schemalessResourceV2, null, "a");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf("a"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nulls() throws IllegalAccessException, InvocationTargetException {
    JsonNode result = (JsonNode) wrapResponseId.invoke(schemalessResourceV2, null, null);
    ObjectNode expected = mapper.createObjectNode();
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nodeIdDefinedNullPageState()
      throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result =
        (JsonNode) wrapResponseIdPageState.invoke(schemalessResourceV2, node, "a", null);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf("a"));
    expected.set("data", node);
    expected.set("pageState", NullNode.getInstance());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nodeDefinedNullPageState()
      throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result =
        (JsonNode) wrapResponseIdPageState.invoke(schemalessResourceV2, node, null, null);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("data", node);
    expected.set("pageState", NullNode.getInstance());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_idDefinedNullPageState()
      throws IllegalAccessException, InvocationTargetException {
    JsonNode result =
        (JsonNode) wrapResponseIdPageState.invoke(schemalessResourceV2, null, "a", null);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf("a"));
    expected.set("pageState", NullNode.getInstance());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nodeIdPageStateDefined()
      throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result =
        (JsonNode)
            wrapResponseIdPageState.invoke(schemalessResourceV2, node, "a", "pagestatevalue");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("data", node);
    expected.set("documentId", TextNode.valueOf("a"));
    expected.set("pageState", TextNode.valueOf("pagestatevalue"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nodePageStateDefined()
      throws IllegalAccessException, InvocationTargetException {
    ObjectNode node = mapper.createObjectNode();
    node.set("name", TextNode.valueOf("Eric"));
    JsonNode result =
        (JsonNode)
            wrapResponseIdPageState.invoke(schemalessResourceV2, node, null, "pagestatevalue");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("data", node);
    expected.set("pageState", TextNode.valueOf("pagestatevalue"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_idPageStateDefined()
      throws IllegalAccessException, InvocationTargetException {
    JsonNode result =
        (JsonNode)
            wrapResponseIdPageState.invoke(schemalessResourceV2, null, "a", "pagestatevalue");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf("a"));
    expected.set("pageState", TextNode.valueOf("pagestatevalue"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_pageStateDefined()
      throws IllegalAccessException, InvocationTargetException {
    JsonNode result =
        (JsonNode)
            wrapResponseIdPageState.invoke(schemalessResourceV2, null, null, "pagestatevalue");
    ObjectNode expected = mapper.createObjectNode();
    expected.set("pageState", TextNode.valueOf("pagestatevalue"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void wrapResponse_nullsPageState()
      throws IllegalAccessException, InvocationTargetException {
    JsonNode result =
        (JsonNode) wrapResponseIdPageState.invoke(schemalessResourceV2, null, null, null);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("pageState", NullNode.getInstance());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void postDoc_success() throws UnauthorizedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String payload = "{}";

    PowerMockito.when(
            schemalessServiceMock.putAtRoot(
                anyString(), anyString(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(true);

    Response r =
        schemalessResourceV2.postDoc(headers, ui, authToken, keyspace, collection, payload);

    assertThat(r.getStatus()).isEqualTo(201);
    mapper.readTree((String) r.getEntity()).requiredAt("/documentId");
  }

  @Test
  public void postDoc_idCollision() throws UnauthorizedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String payload = "{}";

    PowerMockito.when(
            schemalessServiceMock.putAtRoot(
                anyString(), anyString(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(false);

    Response r =
        schemalessResourceV2.postDoc(headers, ui, authToken, keyspace, collection, payload);

    assertThat(r.getStatus()).isEqualTo(500);
    assertThat((String) r.getEntity()).startsWith("Fatal ID collision, try once more: ");
  }

  @Test
  public void putDoc_success() throws UnauthorizedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String payload = "{}";

    PowerMockito.when(
            schemalessServiceMock.putAtRoot(
                anyString(), anyString(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(true);

    Response r =
        schemalessResourceV2.putDoc(headers, ui, authToken, keyspace, collection, id, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    mapper.readTree((String) r.getEntity()).requiredAt("/documentId");
  }

  @Test
  public void putDoc_idCollision() throws UnauthorizedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String payload = "{}";

    PowerMockito.when(
            schemalessServiceMock.putAtRoot(
                anyString(), anyString(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(false);

    Response r =
        schemalessResourceV2.putDoc(headers, ui, authToken, keyspace, collection, id, payload);

    assertThat(r.getStatus()).isEqualTo(409);
    assertThat((String) r.getEntity())
        .startsWith("Document id already exists in collection collection");
  }

  @Test
  public void putDocPath() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    List<PathSegment> path = new ArrayList<>();
    String payload = "{}";

    Response r =
        schemalessResourceV2.putDocPath(
            headers, ui, authToken, keyspace, collection, id, path, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity()).requiredAt("/documentId").asText())
        .isEqualTo(id);
  }

  @Test
  public void patchDoc() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String payload = "{}";

    Response r =
        schemalessResourceV2.patchDoc(headers, ui, authToken, keyspace, collection, id, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity()).requiredAt("/documentId").asText())
        .isEqualTo(id);
  }

  @Test
  public void patchDocPath() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    List<PathSegment> path = new ArrayList<>();
    String payload = "{}";

    Response r =
        schemalessResourceV2.patchDocPath(
            headers, ui, authToken, keyspace, collection, id, path, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity()).requiredAt("/documentId").asText())
        .isEqualTo(id);
  }

  @Test
  public void deleteDoc() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";

    Response r = schemalessResourceV2.deleteDoc(headers, ui, authToken, keyspace, collection, id);

    assertThat(r.getStatus()).isEqualTo(204);
  }

  @Test
  public void deleteDocPath() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    List<PathSegment> path = new ArrayList<>();

    Response r =
        schemalessResourceV2.deleteDocPath(headers, ui, authToken, keyspace, collection, id, path);

    assertThat(r.getStatus()).isEqualTo(204);
  }

  @Test
  public void getDoc() throws ExecutionException, InterruptedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = null;
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;

    ObjectNode mockedReturn = mapper.createObjectNode();
    mockedReturn.set("someData", BooleanNode.valueOf(true));

    PowerMockito.when(
            schemalessServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        schemalessResourceV2.getDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            true);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_rawTrue()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = null;
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    List<PathSegment> path = new ArrayList<>();

    ObjectNode mockedReturn = mapper.createObjectNode();
    mockedReturn.set("someData", BooleanNode.valueOf(true));

    PowerMockito.when(
            schemalessServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        schemalessResourceV2.getDocPath(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            true);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_whereAndFields()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = "{\"a\": {\"$eq\": \"b\"}}";
    String fields = "[\"a\"]";
    int pageSizeParam = 0;
    String pageStateParam = null;
    List<PathSegment> path = new ArrayList<>();

    ObjectNode mockedReturn = mapper.createObjectNode();
    mockedReturn.set("someData", BooleanNode.valueOf(true));

    PowerMockito.when(
            schemalessServiceMock.searchDocumentsV2(
                anyObject(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(ImmutablePair.of(mockedReturn, null));

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenCallRealMethod();

    PowerMockito.when(schemalessServiceMock.convertToSelectionList(anyObject()))
        .thenCallRealMethod();

    Response r =
        schemalessResourceV2.getDocPath(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            true);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_rawFalse()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = null;
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    List<PathSegment> path = new ArrayList<>();

    ObjectNode mockedReturn = mapper.createObjectNode();
    mockedReturn.set("someData", BooleanNode.valueOf(true));

    PowerMockito.when(
            schemalessServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        schemalessResourceV2.getDocPath(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            false);

    assertThat(r.getStatus()).isEqualTo(200);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf(id));
    expected.set("data", mockedReturn);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(expected);
  }

  @Test
  public void getDocPath_emptyResult() throws ExecutionException, InterruptedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = null;
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    List<PathSegment> path = new ArrayList<>();

    PowerMockito.when(
            schemalessServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(null);

    Response r =
        schemalessResourceV2.getDocPath(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            false);
    assertThat(r.getStatus()).isEqualTo(204);
    r =
        schemalessResourceV2.getDocPath(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            true);
    assertThat(r.getStatus()).isEqualTo(204);
  }

  @Test
  public void searchDoc_whereWithNoFields()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = "";
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = "dmFsdWU=";
    boolean raw = false;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field"), "$eq", "value"));

    ObjectNode searchResult = mapper.createObjectNode();
    searchResult.set("id1", mapper.createArrayNode());
    searchResult.set("id2", mapper.createArrayNode());

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    PowerMockito.when(
            schemalessServiceMock.getFullDocumentsFiltered(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenReturn(ImmutablePair.of(searchResult, null));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(200);
    JsonNode resp = mapper.readTree((String) r.getEntity());
    ObjectNode expected = mapper.createObjectNode();
    expected.set("data", searchResult);
    expected.set("pageState", null);
    assertThat(resp).isEqualTo(expected);
  }

  @Test
  public void searchDoc_whereWithNoFieldsRaw()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = "";
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    boolean raw = true;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field"), "$eq", "value"));

    ObjectNode searchResult = mapper.createObjectNode();
    searchResult.set("id1", mapper.createArrayNode());
    searchResult.set("id2", mapper.createArrayNode());

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    PowerMockito.when(
            schemalessServiceMock.getFullDocumentsFiltered(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenReturn(ImmutablePair.of(searchResult, null));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(searchResult);
  }

  @Test
  public void searchDoc_whereWithFieldsRaw()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = "";
    String fields = "";
    int pageSizeParam = 0;
    String pageStateParam = null;
    boolean raw = true;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field1"), "$eq", "value"));

    ObjectNode searchResult = mapper.createObjectNode();
    searchResult.set("id1", mapper.createArrayNode());
    searchResult.set("id2", mapper.createArrayNode());

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    PowerMockito.when(schemalessServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    PowerMockito.when(
            schemalessServiceMock.getFullDocumentsFiltered(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenReturn(ImmutablePair.of(searchResult, null));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(searchResult);
  }

  @Test
  public void searchDoc_invalidFieldsWithNoWhere() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = null;
    String fields = "";
    int pageSizeParam = 0;
    String pageStateParam = null;
    boolean raw = true;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field1"), "$eq", "value"));

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);
    PowerMockito.when(schemalessServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(204);
  }

  @Test
  public void searchDoc_invalidMultipleWhereFields() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = "";
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    boolean raw = true;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field1"), "$eq", "value"));
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field2"), "$eq", "value"));

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(400);
    assertThat((String) r.getEntity())
        .startsWith("Conditions across multiple fields are not yet supported (found: ");
  }

  @Test
  public void searchDoc_invalidPageSize() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = "";
    String fields = "";
    int pageSizeParam = 21;
    String pageStateParam = null;
    boolean raw = true;

    List<FilterCondition> conditions = new ArrayList<>();
    conditions.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c", "field1"), "$eq", "value"));

    PowerMockito.when(schemalessServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    PowerMockito.when(schemalessServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(400);
    assertThat((String) r.getEntity()).isEqualTo("The parameter `page-size` is limited to 20.");
  }

  @Test
  public void searchDoc_fullDocuments()
      throws InterruptedException, ExecutionException, UnauthorizedException,
          JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = null;
    String fields = null;
    int pageSizeParam = 0;
    String pageStateParam = null;
    boolean raw = true;

    ObjectNode searchResult = mapper.createObjectNode();
    searchResult.set("id1", mapper.createArrayNode());
    searchResult.set("id2", mapper.createArrayNode());

    PowerMockito.when(
            schemalessServiceMock.getFullDocuments(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenReturn(ImmutablePair.of(searchResult, null));

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(searchResult);
  }

  @Test
  public void searchDoc_invalidPageSizeFullDocuments() {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String where = null;
    String fields = null;
    int pageSizeParam = 21;
    String pageStateParam = null;
    boolean raw = true;

    Response r =
        schemalessResourceV2.searchDoc(
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            where,
            fields,
            pageSizeParam,
            pageStateParam,
            raw);
    assertThat(r.getStatus()).isEqualTo(400);
    assertThat((String) r.getEntity()).isEqualTo("The parameter `page-size` is limited to 20.");
  }
}
