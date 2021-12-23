package io.stargate.web.docsapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.web.docsapi.dao.DocumentDBFactory;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.DocsSchemaChecker;
import io.stargate.web.docsapi.service.DocumentService;
import io.stargate.web.restapi.dao.RestDB;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DocumentResourceV2Test {
  private final RestDB restDBMock = mock(RestDB.class);
  @InjectMocks private DocumentResourceV2 documentResourceV2;
  @Mock private DocumentService documentServiceMock;
  @Mock private DocumentDBFactory dbFactoryMock;
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
  public void postMultiDoc_success() throws JsonProcessingException {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(anyString())).thenReturn("application/json");
    UriInfo ui = mock(UriInfo.class);
    String authToken = "auth_token";
    String keyspace = "keyspace";
    String collection = "collection";
    InputStream payload = mock(InputStream.class);

    Response r =
        documentResourceV2.writeManyDocs(
            headers, ui, authToken, keyspace, collection, payload, null, false, httpServletRequest);

    assertThat(r.getStatus()).isEqualTo(202);
    mapper.readTree((String) r.getEntity()).requiredAt("/documentIds");
  }

  @Test
  public void putDoc_success() throws JsonProcessingException {
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
}
