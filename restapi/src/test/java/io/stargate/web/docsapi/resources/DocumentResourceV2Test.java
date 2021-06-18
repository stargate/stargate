package io.stargate.web.docsapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Db;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DocumentResourceV2Test {
  private final AuthenticatedDB authenticatedDBMock = mock(AuthenticatedDB.class);
  @InjectMocks private DocumentResourceV2 documentResourceV2;
  @Mock private DocumentService documentServiceMock;
  @Mock private Db dbFactoryMock;
  @Mock private DocsSchemaChecker schemaCheckerMock;
  @Spy private ObjectMapper mapper = new ObjectMapper();
  @Spy private DocsApiConfiguration conf;
  private HttpServletRequest httpServletRequest;

  @BeforeEach
  public void setup() {
    httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
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

    Response r =
        documentResourceV2.postDoc(
            headers, ui, authToken, keyspace, collection, payload, false, httpServletRequest);

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
        documentResourceV2.putDoc(
            headers, ui, authToken, keyspace, collection, id, payload, false, httpServletRequest);

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
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            payload,
            false,
            httpServletRequest);

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
        documentResourceV2.patchDoc(
            headers, ui, authToken, keyspace, collection, id, payload, false, httpServletRequest);

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
            headers,
            ui,
            authToken,
            keyspace,
            collection,
            id,
            path,
            payload,
            false,
            httpServletRequest);

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

    Response r =
        documentResourceV2.deleteDoc(
            headers, ui, authToken, keyspace, collection, id, httpServletRequest);

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
        documentResourceV2.deleteDocPath(
            headers, ui, authToken, keyspace, collection, id, path, httpServletRequest);

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
                anyObject(), anyString(), anyString(), anyString(), anyObject(), any()))
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
            false,
            true,
            httpServletRequest);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_rawTrue() throws JsonProcessingException, UnauthorizedException {
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
                anyObject(), anyString(), anyString(), anyString(), anyObject(), any()))
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
            false,
            true,
            httpServletRequest);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_whereAndFields() throws JsonProcessingException, UnauthorizedException {
    HttpHeaders headers = mock(HttpHeaders.class);
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    String id = "id";
    String where = "{\"a\": {\"$eq\": \"b\"}}";
    String fields = "[\"a\"]";
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
                anyObject(),
                any()))
        .thenReturn(mockedReturn);

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
            null,
            pageStateParam,
            false,
            true,
            httpServletRequest);

    assertThat(r.getStatus()).isEqualTo(200);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(mockedReturn);
  }

  @Test
  public void getDocPath_rawFalse() throws JsonProcessingException, UnauthorizedException {
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
                anyObject(), anyString(), anyString(), anyString(), anyObject(), any()))
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
            false,
            false,
            httpServletRequest);

    assertThat(r.getStatus()).isEqualTo(200);
    ObjectNode expected = mapper.createObjectNode();
    expected.set("documentId", TextNode.valueOf(id));
    expected.set("data", mockedReturn);
    assertThat(mapper.readTree((String) r.getEntity())).isEqualTo(expected);
  }

  @Test
  public void getDocPath_emptyResult() throws UnauthorizedException {
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
                anyObject(), anyString(), anyString(), anyString(), anyObject(), any()))
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
            false,
            false,
            httpServletRequest);
    assertThat(r.getStatus()).isEqualTo(404);
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
            false,
            true,
            httpServletRequest);
    assertThat(r.getStatus()).isEqualTo(404);
  }
}
