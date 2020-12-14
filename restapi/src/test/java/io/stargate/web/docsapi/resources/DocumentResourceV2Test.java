package io.stargate.web.docsapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.resources.Db;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DocumentResourceV2Test {
  private static final ObjectMapper mapper = new ObjectMapper();
  private final DocumentService documentServiceMock = mock(DocumentService.class);
  private final Db dbFactoryMock = mock(Db.class);
  private DocumentResourceV2 documentResourceV2;

  @BeforeEach
  public void setup() {
    documentResourceV2 = new DocumentResourceV2(dbFactoryMock, documentServiceMock);
  }

  @Test
  public void postDoc_success() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String payload = "{}";

    Response r = documentResourceV2.postDoc(headers, ui, authToken, keyspace, collection, payload);

    assertThat(r.getStatus()).isEqualTo(201);
    mapper.readTree((String) r.getEntity()).requiredAt("/documentId");
  }

  @Test
  public void putDoc_success() throws UnauthorizedException, JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String payload = "{}";

    Response r =
        documentResourceV2.putDoc(headers, ui, authToken, keyspace, collection, id, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    mapper.readTree((String) r.getEntity()).requiredAt("/documentId");
  }

  @Test
  public void putDocPath() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    List<PathSegment> path = new ArrayList<>();
    String payload = "{}";

    Response r =
        documentResourceV2.putDocPath(
            headers, ui, authToken, keyspace, collection, id, path, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity()).requiredAt("/documentId").asText())
        .isEqualTo(id);
  }

  @Test
  public void patchDoc() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String payload = "{}";

    Response r =
        documentResourceV2.patchDoc(headers, ui, authToken, keyspace, collection, id, payload);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity()).requiredAt("/documentId").asText())
        .isEqualTo(id);
  }

  @Test
  public void patchDocPath() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    List<PathSegment> path = new ArrayList<>();
    String payload = "{}";

    Response r =
        documentResourceV2.patchDocPath(
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

    Response r = documentResourceV2.deleteDoc(headers, ui, authToken, keyspace, collection, id);

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
        documentResourceV2.deleteDocPath(headers, ui, authToken, keyspace, collection, id, path);

    assertThat(r.getStatus()).isEqualTo(204);
  }

  @Test
  public void getDoc()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
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

    Mockito.when(
            documentServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        documentResourceV2.getDoc(
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
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
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

    Mockito.when(
            documentServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        documentResourceV2.getDocPath(
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
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
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

    Mockito.when(
            documentServiceMock.searchDocumentsV2(
                anyObject(),
                anyString(),
                anyString(),
                anyList(),
                anyList(),
                anyString(),
                anyInt(),
                anyObject()))
        .thenReturn(ImmutablePair.of(mockedReturn, null));

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenCallRealMethod();

    Mockito.when(documentServiceMock.convertToSelectionList(anyObject())).thenCallRealMethod();

    Response r =
        documentResourceV2.getDocPath(
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
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
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

    Mockito.when(
            documentServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(mockedReturn);

    Response r =
        documentResourceV2.getDocPath(
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
  public void getDocPath_emptyResult()
      throws ExecutionException, InterruptedException, UnauthorizedException {
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

    Mockito.when(
            documentServiceMock.getJsonAtPath(
                anyObject(), anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(null);

    Response r =
        documentResourceV2.getDocPath(
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
        documentResourceV2.getDocPath(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Mockito.when(
            documentServiceMock.getFullDocumentsFiltered(
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
        documentResourceV2.searchDoc(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Mockito.when(
            documentServiceMock.getFullDocumentsFiltered(
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
        documentResourceV2.searchDoc(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Mockito.when(documentServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    Mockito.when(
            documentServiceMock.getFullDocumentsFiltered(
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
        documentResourceV2.searchDoc(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);
    Mockito.when(documentServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    Response r =
        documentResourceV2.searchDoc(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Response r =
        documentResourceV2.searchDoc(
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

    Mockito.when(documentServiceMock.convertToFilterOps(anyList(), anyObject()))
        .thenReturn(conditions);

    Mockito.when(documentServiceMock.convertToSelectionList(anyObject()))
        .thenReturn(ImmutableList.of("field1"));

    Response r =
        documentResourceV2.searchDoc(
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

    Mockito.when(
            documentServiceMock.getFullDocuments(
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
        documentResourceV2.searchDoc(
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
        documentResourceV2.searchDoc(
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
