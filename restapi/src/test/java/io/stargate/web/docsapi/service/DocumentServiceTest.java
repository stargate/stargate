package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.ListFilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.resources.Db;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferGson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DocumentServiceTest {
  private final DataStore dataStore = Mockito.mock(DataStore.class);
  @InjectMocks DocumentService service;
  @Mock JsonConverter jsonConverter;
  @Spy ObjectMapper serviceMapper;
  @Spy DocsApiConfiguration docsConfig;

  private Method convertToBracketedPath;
  private Method leftPadTo6;
  private Method convertArrayPath;
  private Method isEmptyObject;
  private Method isEmptyArray;
  private Method shredPayload;
  private Method validateOpAndValue;
  private Method addRowsToMap;
  private Method updateExistenceForMap;
  private Method getParentPathFromRow;
  private Method filterToSelectionSet;
  private Method applyInMemoryFilters;
  private Method pathsMatch;
  private Method checkEqualsOp;
  private Method checkInOp;
  private Method checkGtOp;
  private Method checkLtOp;
  private Method searchRows;

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

  @BeforeEach
  public void setup() throws NoSuchMethodException {
    convertToBracketedPath =
        DocumentService.class.getDeclaredMethod("convertToBracketedPath", String.class);
    convertToBracketedPath.setAccessible(true);
    leftPadTo6 = DocumentService.class.getDeclaredMethod("leftPadTo6", String.class);
    leftPadTo6.setAccessible(true);
    convertArrayPath = DocumentService.class.getDeclaredMethod("convertArrayPath", String.class);
    convertArrayPath.setAccessible(true);
    isEmptyObject = DocumentService.class.getDeclaredMethod("isEmptyObject", Object.class);
    isEmptyObject.setAccessible(true);
    isEmptyArray = DocumentService.class.getDeclaredMethod("isEmptyArray", Object.class);
    isEmptyArray.setAccessible(true);
    shredPayload =
        DocumentService.class.getDeclaredMethod(
            "shredPayload",
            JsonSurfer.class,
            DocumentDB.class,
            List.class,
            String.class,
            String.class,
            boolean.class,
            boolean.class);
    shredPayload.setAccessible(true);
    validateOpAndValue =
        DocumentService.class.getDeclaredMethod(
            "validateOpAndValue", String.class, JsonNode.class, String.class);
    validateOpAndValue.setAccessible(true);
    addRowsToMap = DocumentService.class.getDeclaredMethod("addRowsToMap", Map.class, List.class);
    addRowsToMap.setAccessible(true);
    updateExistenceForMap =
        DocumentService.class.getDeclaredMethod(
            "updateExistenceForMap",
            Set.class,
            List.class,
            List.class,
            boolean.class,
            boolean.class);
    updateExistenceForMap.setAccessible(true);
    getParentPathFromRow =
        DocumentService.class.getDeclaredMethod("getParentPathFromRow", Row.class);
    getParentPathFromRow.setAccessible(true);
    filterToSelectionSet =
        DocumentService.class.getDeclaredMethod(
            "filterToSelectionSet", List.class, List.class, List.class);
    filterToSelectionSet.setAccessible(true);
    applyInMemoryFilters =
        DocumentService.class.getDeclaredMethod(
            "applyInMemoryFilters", List.class, List.class, int.class, boolean.class);
    applyInMemoryFilters.setAccessible(true);
    pathsMatch = DocumentService.class.getDeclaredMethod("pathsMatch", String.class, String.class);
    pathsMatch.setAccessible(true);
    checkEqualsOp =
        DocumentService.class.getDeclaredMethod(
            "checkEqualsOp",
            SingleFilterCondition.class,
            String.class,
            Boolean.class,
            Double.class);
    checkEqualsOp.setAccessible(true);
    checkInOp =
        DocumentService.class.getDeclaredMethod(
            "checkInOp", ListFilterCondition.class, String.class, Boolean.class, Double.class);
    checkInOp.setAccessible(true);
    checkGtOp =
        DocumentService.class.getDeclaredMethod(
            "checkGtOp", SingleFilterCondition.class, String.class, Boolean.class, Double.class);
    checkGtOp.setAccessible(true);

    checkLtOp =
        DocumentService.class.getDeclaredMethod(
            "checkLtOp", SingleFilterCondition.class, String.class, Boolean.class, Double.class);
    checkLtOp.setAccessible(true);
  }

  @Test
  public void convertToBracketedPath() throws IllegalAccessException, InvocationTargetException {
    String input = "$.a.b";
    String res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['a']['b']");

    input = "a.b";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("['a']['b']");

    input = "$.$.c";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['$']['c']");

    input = "$.a.b[0].c";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['a']['b'][0]['c']");

    input = "$.a.I need some space.c";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['a']['I need some space']['c']");

    input = "$.a.0[0].c";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['a']['0'][0]['c']");

    input = "$.@.23.f";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['@']['23']['f']");

    // This should really never be sent to this function...but it does "work"
    input = "$..a";
    res = (String) convertToBracketedPath.invoke(service, input);
    assertThat(res).isEqualTo("$['']['a']");
  }

  @Test
  public void leftPadTo6() throws InvocationTargetException, IllegalAccessException {
    String result = (String) leftPadTo6.invoke(service, "");
    assertThat(result).isEqualTo("000000");

    result = (String) leftPadTo6.invoke(service, "00");
    assertThat(result).isEqualTo("000000");

    result = (String) leftPadTo6.invoke(service, "1");
    assertThat(result).isEqualTo("000001");

    result = (String) leftPadTo6.invoke(service, "abc");
    assertThat(result).isEqualTo("000abc");

    result = (String) leftPadTo6.invoke(service, "AbCd");
    assertThat(result).isEqualTo("00AbCd");
  }

  @Test
  public void convertArrayPath() throws InvocationTargetException, IllegalAccessException {
    String result = (String) convertArrayPath.invoke(service, "");
    assertThat(result).isEqualTo("");

    result = (String) convertArrayPath.invoke(service, "a");
    assertThat(result).isEqualTo("a");

    result = (String) convertArrayPath.invoke(service, "[0]");
    assertThat(result).isEqualTo("[000000]");

    result = (String) convertArrayPath.invoke(service, "[0101]");
    assertThat(result).isEqualTo("[000101]");

    result = (String) convertArrayPath.invoke(service, "[999999]");
    assertThat(result).isEqualTo("[999999]");
  }

  @Test
  public void convertArrayPath_invalidInput() {
    Throwable thrown = catchThrowable(() -> convertArrayPath.invoke(service, "[]"));
    assertThat(thrown.getCause())
        .isInstanceOf(NumberFormatException.class)
        .hasMessage("For input string: \"\"");

    thrown = catchThrowable(() -> convertArrayPath.invoke(service, "[a]"));
    assertThat(thrown.getCause())
        .isInstanceOf(NumberFormatException.class)
        .hasMessage("For input string: \"a\"");

    thrown = catchThrowable(() -> convertArrayPath.invoke(service, "[1000000]"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessage("Max array length of 1000000 exceeded.");
  }

  @Test
  public void isEmptyObject() throws InvocationTargetException, IllegalAccessException {
    assertThat(isEmptyObject.invoke(service, "a")).isEqualTo(false);
    assertThat(isEmptyObject.invoke(service, (Object) null)).isEqualTo(false);
    assertThat(isEmptyObject.invoke(service, JsonNull.INSTANCE)).isEqualTo(false);
    assertThat(isEmptyObject.invoke(service, new JsonArray())).isEqualTo(false);
    JsonArray nonEmptyArray = new JsonArray();
    nonEmptyArray.add("something");
    assertThat(isEmptyObject.invoke(service, nonEmptyArray)).isEqualTo(false);
    JsonObject nonEmptyObj = new JsonObject();
    nonEmptyObj.add("key", new JsonPrimitive("something"));
    assertThat(isEmptyObject.invoke(service, nonEmptyObj)).isEqualTo(false);
    assertThat(isEmptyObject.invoke(service, new JsonObject())).isEqualTo(true);
  }

  @Test
  public void isEmptyArray() throws InvocationTargetException, IllegalAccessException {
    assertThat(isEmptyArray.invoke(service, "a")).isEqualTo(false);
    assertThat(isEmptyArray.invoke(service, (Object) null)).isEqualTo(false);
    assertThat(isEmptyArray.invoke(service, JsonNull.INSTANCE)).isEqualTo(false);
    JsonObject nonEmptyObj = new JsonObject();
    nonEmptyObj.add("key", new JsonPrimitive("something"));
    assertThat(isEmptyArray.invoke(service, nonEmptyObj)).isEqualTo(false);
    assertThat(isEmptyArray.invoke(service, new JsonObject())).isEqualTo(false);
    JsonArray nonEmptyArray = new JsonArray();
    nonEmptyArray.add("something");
    assertThat(isEmptyArray.invoke(service, nonEmptyArray)).isEqualTo(false);
    assertThat(isEmptyArray.invoke(service, new JsonArray())).isEqualTo(true);
  }

  @Test
  public void shredPayload_booleanLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": true}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      null,
      null,
      true,
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_numberLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": 3}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      null,
      3.0,
      null
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_stringLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": \"leaf\"}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      "leaf",
      null,
      null
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_emptyObjectLeaf()
      throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": {}}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      DocumentDB.EMPTY_OBJECT_MARKER,
      null,
      null,
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_emptyArrayLeaf()
      throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": []}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      DocumentDB.EMPTY_ARRAY_MARKER,
      null,
      null
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_nullLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": null}}";
    ImmutablePair<?, ?> shredResult =
        (ImmutablePair<?, ?>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true);
    List<?> bindVariables = (List<?>) shredResult.left;
    List<?> topLevelKeys = (List<?>) shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = (Object[]) bindVariables.get(0);
    assertThat(vars.length).isEqualTo(69);
    Object[] expected = {
      "eric",
      "cool",
      "document",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "document",
      null,
      null,
      null
    };
    for (int i = 0; i < vars.length; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_invalidKeys() {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"coo]\": {\"document\": null}}";
    Throwable thrown =
        catchThrowable(
            () ->
                shredPayload.invoke(
                    service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, false, true));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("are not permitted in JSON field names, invalid field coo]");
  }

  @Test
  public void shredPayload_patchingArrayInvalid() {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "[1, 2, 3]";
    Throwable thrown =
        catchThrowable(
            () ->
                shredPayload.invoke(
                    service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, true, true));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("A patch operation must be done with a JSON object, not an array.");
  }

  @Test
  public void putAtPath() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString(), any())).thenReturn(dbMock);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    service.putAtPath(
        "authToken",
        "ks",
        "collection",
        "id",
        "{\"some\": \"data\"}",
        new ArrayList<>(),
        false,
        dbFactoryMock,
        true,
        EMPTY_HEADERS);

    verify(dbMock, times(1))
        .deleteThenInsertBatch(anyString(), anyString(), anyString(), any(), any(), anyLong());
    verify(dbMock, times(0))
        .deletePatchedPathsThenInsertBatch(
            anyString(), anyString(), anyString(), any(), any(), any(), anyLong());
  }

  @Test
  public void putAtPath_patching() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString(), any())).thenReturn(dbMock);
    when(dbMock.newBindMap(any())).thenCallRealMethod();

    service.putAtPath(
        "authToken",
        "ks",
        "collection",
        "id",
        "{\"some\": \"data\"}",
        new ArrayList<>(),
        true,
        dbFactoryMock,
        true,
        EMPTY_HEADERS);

    verify(dbMock, times(0))
        .deleteThenInsertBatch(anyString(), anyString(), anyString(), any(), any(), anyLong());
    verify(dbMock, times(1))
        .deletePatchedPathsThenInsertBatch(
            anyString(), anyString(), anyString(), any(), any(), any(), anyLong());
  }

  @Test
  public void putAtPath_noData() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString(), any())).thenReturn(dbMock);

    Throwable thrown =
        catchThrowable(
            () ->
                service.putAtPath(
                    "authToken",
                    "ks",
                    "collection",
                    "id",
                    "\"a\"",
                    new ArrayList<>(),
                    true,
                    dbFactoryMock,
                    true,
                    EMPTY_HEADERS));

    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed.");
  }

  @Test
  public void getJsonAtPath() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);
    when(rsMock.rows()).thenReturn(new ArrayList<>());

    List<PathSegment> path = smallPath();
    JsonNode result = service.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRowsEmptyJson() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);

    List<Row> rows = makeInitialRowData(false);
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    when(jsonConverter.convertToJsonDoc(anyList(), any(), anyBoolean(), anyBoolean()))
        .thenReturn(mapper.createObjectNode());

    JsonNode result = service.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRows() throws JsonProcessingException, UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);

    List<Row> rows = makeInitialRowData(false);
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    ObjectNode jsonObj = mapper.createObjectNode();
    jsonObj.set("abc", mapper.readTree("[1]"));

    when(jsonConverter.convertToJsonDoc(anyListOf(Row.class), any(), anyBoolean(), anyBoolean()))
        .thenReturn(jsonObj);

    JsonNode result = service.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isEqualTo(IntNode.valueOf(1));
  }

  @Test
  public void validateOpAndValue()
      throws JsonProcessingException, InvocationTargetException, IllegalAccessException {
    final JsonNode value = mapper.readTree("{\"a\": \"b\"}");
    validateOpAndValue.invoke(service, "not_valid", value, "field");

    Throwable thrown =
        catchThrowable(() -> validateOpAndValue.invoke(service, "$ne", value, "field"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("was expecting a value or `null`");

    thrown = catchThrowable(() -> validateOpAndValue.invoke(service, "$exists", value, "field"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("only supports the value `true`");

    final JsonNode value2 = mapper.readTree("false");
    thrown = catchThrowable(() -> validateOpAndValue.invoke(service, "$exists", value2, "field"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("only supports the value `true`");

    thrown = catchThrowable(() -> validateOpAndValue.invoke(service, "$gt", value, "field"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("was expecting a non-null value");

    thrown = catchThrowable(() -> validateOpAndValue.invoke(service, "$in", value, "field"));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("was expecting an array");
  }

  @Test
  public void convertToSelectionList() throws JsonProcessingException {
    JsonNode input = mapper.readTree("[\"A\", \"B\"]");
    List<String> res = service.convertToSelectionList(input);
    List<String> expected = new ArrayList<>();
    expected.add("A");
    expected.add("B");
    assertThat(res).isEqualTo(expected);
  }

  @Test
  public void convertToSelectionList_invalidInput() throws JsonProcessingException {
    final JsonNode input = mapper.readTree("{}");
    Throwable thrown = catchThrowable(() -> service.convertToSelectionList(input));
    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("`fields` must be a JSON array, found ");

    final JsonNode input2 = mapper.readTree("[\"A\", 0]");
    thrown = catchThrowable(() -> service.convertToSelectionList(input2));
    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("Each field must be a string, found ");
  }

  @Test
  public void convertToFilterOps() throws JsonProcessingException {
    JsonNode input =
        mapper.readTree(
            "{\"a.b.c\": {\"$eq\": 1, \"$lt\": true, \"$lte\": \"1\", \"$ne\": null, \"$in\": [1, \"a\", true]}}");
    List<FilterCondition> result = service.convertToFilterOps(new ArrayList<>(), input);
    List<FilterCondition> expected = new ArrayList<>();
    expected.add(
        new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", Double.valueOf(1)));
    expected.add(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lt", true));
    expected.add(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lte", "1"));
    expected.add(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$ne", (String) null));
    expected.add(
        new ListFilterCondition(
            ImmutableList.of("a", "b", "c"), "$in", ImmutableList.of(1, "a", true)));
    assertThat(result.toString()).isEqualTo(expected.toString());
  }

  @Test
  public void convertToFilterOps_invalidInput() throws JsonProcessingException {
    final JsonNode input = mapper.readTree("[]");
    Throwable thrown = catchThrowable(() -> service.convertToFilterOps(new ArrayList<>(), input));
    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessage("Search was expecting a JSON object as input.");

    final JsonNode input2 = mapper.readTree("{\"\": {}}");
    thrown = catchThrowable(() -> service.convertToFilterOps(new ArrayList<>(), input2));
    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessage("The field(s) you are searching for can't be the empty string!");

    final JsonNode input3 = mapper.readTree("{\"a\": []}}");
    thrown = catchThrowable(() -> service.convertToFilterOps(new ArrayList<>(), input3));
    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessage("Search entry for field a was expecting a JSON object as input.");
  }

  @Test
  public void deleteAtPath() throws UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);

    service.deleteAtPath(dbMock, "keyspace", "collection", "id", smallPath());
    verify(dbMock, times(1))
        .delete(anyString(), anyString(), anyString(), anyListOf(String.class), anyLong());
  }

  @Test
  public void searchDocumentsV2_emptyResult() throws Exception {
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    Mockito.when(serviceMock.searchWithinDocument(any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();
    Mockito.when(serviceMock.getRowsForDocument(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(new ArrayList<>());
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    JsonNode result =
        serviceMock.searchWithinDocument(
            dbMock, "keyspace", "collection", "1", filters, new ArrayList<>(), paginator);
    assertThat(result).isNull();
  }

  @Test
  public void searchDocumentsV2_existingResult() throws Exception {
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData(false);
    when(dbMock.executeSelect(anyString(), anyString(), any(), anyBoolean(), anyInt(), any()))
        .thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    JsonNode result =
        service.searchWithinDocument(
            dbMock, "keyspace", "collection", "1", filters, new ArrayList<>(), paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result.isMissingNode()).isFalse();
  }

  @Test
  public void addRowsToMap() throws InvocationTargetException, IllegalAccessException {
    Map<String, List<Row>> rowsByDoc = new HashMap<>();
    List<Row> rows = makeInitialRowData(false);
    addRowsToMap.invoke(service, rowsByDoc, rows);
    assertThat(rowsByDoc.get("1")).isEqualTo(rows);
  }

  @Test
  public void updateExistenceForMap() throws InvocationTargetException, IllegalAccessException {
    Set<String> existenceByDoc = new HashSet<>();
    List<Row> rows = makeInitialRowData(false);
    updateExistenceForMap.invoke(service, existenceByDoc, rows, new ArrayList<>(), false, true);
    assertThat(existenceByDoc.contains("1")).isTrue();
  }

  @Test
  public void getFullDocuments_lessThanLimit() throws Exception {
    DocumentDB dbMock = mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData(false);
    when(dbMock.executeSelectAll(anyString(), anyString(), anyInt(), any())).thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);
    Mockito.when(jsonConverter.convertToJsonDoc(anyList(), anyBoolean(), anyBoolean()))
        .thenReturn(mapper.readTree("{\"a\": 1}"));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    JsonNode result =
        service.getFullDocuments(dbMock, "keyspace", "collection", new ArrayList<>(), paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void getFullDocuments_greaterThanLimit() throws Exception {
    DocumentDB dbMock = mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData(false);
    when(dbMock.executeSelectAll(anyString(), anyString(), anyInt(), any())).thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);
    List<Row> twoDocsRows = makeInitialRowData(false);
    twoDocsRows.addAll(makeRowDataForSecondDoc(false));
    Mockito.when(jsonConverter.convertToJsonDoc(anyList(), anyBoolean(), anyBoolean()))
        .thenReturn(mapper.readTree("{\"a\": 1}"));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    JsonNode result =
        service.getFullDocuments(dbMock, "keyspace", "collection", new ArrayList<>(), paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void getUnfilteredRows() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData(false);
    when(dbMock.executeSelectAll(anyString(), anyString(), anyInt(), any())).thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);

    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<Row> result = service.getUnfilteredRows("keyspace", "collection", dbMock, paginator);

    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(rows);
  }

  @Test
  public void getRowsForDocument() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData(false);
    when(dbMock.executeSelect(anyString(), anyString(), any(), anyBoolean(), anyInt(), any()))
        .thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);
    List<FilterCondition> filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a,b", "*", "c"), "$eq", true));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);
    List<Row> result =
        service.getRowsForDocument(
            "keyspace",
            "collection",
            "docId",
            dbMock,
            filters,
            ImmutableList.of("a", "b", "c"),
            paginator);

    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(Collections.emptyList());
  }

  @Test
  public void getParentPathFromRow() throws InvocationTargetException, IllegalAccessException {
    Row row = makeInitialRowData(false).get(1);
    String result = (String) getParentPathFromRow.invoke(service, row);
    assertThat(result).isEqualTo("1/a.b.");
  }

  @Test
  public void filterToSelectionSet() throws InvocationTargetException, IllegalAccessException {
    List<Row> rows = makeInitialRowData(false);
    rows = rows.subList(1, rows.size());

    List<?> result =
        (List<?>)
            filterToSelectionSet.invoke(
                service, rows, new ArrayList<>(), ImmutableList.of("a", "b"));
    assertThat(result).isEqualTo(rows);

    List<String> selectionSet = ImmutableList.of("c", "n1", "n2", "n3");
    result =
        (List<?>)
            filterToSelectionSet.invoke(service, rows, selectionSet, ImmutableList.of("a", "b"));
    List<Row> expected = new ArrayList<>();
    expected.add(rows.get(0));
    for (int i = 0; i < 7; i++) {
      expected.add(null);
    }
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void applyInMemoryFilters() throws InvocationTargetException, IllegalAccessException {
    List<Row> rows = makeInitialRowData(false);
    rows = rows.subList(1, rows.size());
    List<FilterCondition> filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", true));
    List<?> result =
        (List<?>) applyInMemoryFilters.invoke(service, rows, new ArrayList<>(), 1, false);
    assertThat(result).isEqualTo(rows);

    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isEqualTo(rows.get(0));

    filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$exists", true));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isEqualTo(rows.get(0));

    filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "x"), "$exists", true));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$gt", true));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$gte", 2.0));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lt", true));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lte", false));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$ne", true));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(
            new ListFilterCondition(
                ImmutableList.of("a", "b", "c"), "$in", ImmutableList.of(false)));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(
            new ListFilterCondition(
                ImmutableList.of("a", "b", "c"), "$nin", ImmutableList.of(true)));
    result = (List<?>) applyInMemoryFilters.invoke(service, rows, filters, 1, false);
    assertThat(result.size()).isEqualTo(0);
  }

  @Test
  public void pathsMatch() throws InvocationTargetException, IllegalAccessException {
    boolean res = (boolean) pathsMatch.invoke(service, "", "");
    assertThat(res).isTrue();

    res = (boolean) pathsMatch.invoke(service, "a", "a");
    assertThat(res).isTrue();

    res = (boolean) pathsMatch.invoke(service, "a", "b");
    assertThat(res).isFalse();

    res = (boolean) pathsMatch.invoke(service, "a.b", "a.b");
    assertThat(res).isTrue();

    res = (boolean) pathsMatch.invoke(service, "a.b", "a.c");
    assertThat(res).isFalse();

    res = (boolean) pathsMatch.invoke(service, "a.*.c", "a.b.c");
    assertThat(res).isTrue();

    res = (boolean) pathsMatch.invoke(service, "a.*.d", "a.b.c");
    assertThat(res).isFalse();
  }

  @Test
  public void checkEqualsOp() throws InvocationTargetException, IllegalAccessException {
    SingleFilterCondition cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", true);
    Boolean res = (Boolean) checkEqualsOp.invoke(service, cond, null, true, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", "a");
    res = (Boolean) checkEqualsOp.invoke(service, cond, "a", null, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", 3.14);
    res = (Boolean) checkEqualsOp.invoke(service, cond, null, null, 3.14);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", false);
    res = (Boolean) checkEqualsOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", 3.0);
    res = (Boolean) checkEqualsOp.invoke(service, cond, null, null, 3.1);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", "A");
    res = (Boolean) checkEqualsOp.invoke(service, cond, "a", null, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", "A");
    res = (Boolean) checkEqualsOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();
  }

  @Test
  public void checkInOp() throws InvocationTargetException, IllegalAccessException {
    ListFilterCondition cond =
        new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of("a", "b", "c"));
    Boolean res = (Boolean) checkInOp.invoke(service, cond, "a", null, null);
    assertThat(res).isTrue();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(true));
    res = (Boolean) checkInOp.invoke(service, cond, null, true, null);
    assertThat(res).isTrue();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(4.5));
    res = (Boolean) checkInOp.invoke(service, cond, null, null, 4.5);
    assertThat(res).isTrue();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of("a", "b"));
    res = (Boolean) checkInOp.invoke(service, cond, "c", null, null);
    assertThat(res).isFalse();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(true));
    res = (Boolean) checkInOp.invoke(service, cond, null, false, null);
    assertThat(res).isFalse();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(4.5));
    res = (Boolean) checkInOp.invoke(service, cond, null, null, 4.4);
    assertThat(res).isFalse();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(4.5));
    res = (Boolean) checkInOp.invoke(service, cond, null, null, null);
    assertThat(res).isFalse();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", ImmutableList.of(4.5));
    res = (Boolean) checkInOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();

    cond = new ListFilterCondition(ImmutableList.of("a"), "$in", new ArrayList<>());
    res = (Boolean) checkInOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();
  }

  @Test
  public void checkGtOp() throws InvocationTargetException, IllegalAccessException {
    SingleFilterCondition cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", false);
    Boolean res = (Boolean) checkGtOp.invoke(service, cond, null, true, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", "a");
    res = (Boolean) checkGtOp.invoke(service, cond, "b", null, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", 3.14);
    res = (Boolean) checkGtOp.invoke(service, cond, null, null, 3.141);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", true);
    res = (Boolean) checkGtOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", 3.2);
    res = (Boolean) checkGtOp.invoke(service, cond, null, null, 3.1);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$gt", "b");
    res = (Boolean) checkGtOp.invoke(service, cond, "a", null, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$eq", "A");
    res = (Boolean) checkGtOp.invoke(service, cond, null, true, null);
    assertThat(res).isNull();
  }

  @Test
  public void checkLtOp() throws InvocationTargetException, IllegalAccessException {
    SingleFilterCondition cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", true);
    Boolean res = (Boolean) checkLtOp.invoke(service, cond, null, false, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", "b");
    res = (Boolean) checkLtOp.invoke(service, cond, "a", null, null);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", 3.14);
    res = (Boolean) checkLtOp.invoke(service, cond, null, null, 3.13);
    assertThat(res).isTrue();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", true);
    res = (Boolean) checkLtOp.invoke(service, cond, null, true, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", 3.2);
    res = (Boolean) checkLtOp.invoke(service, cond, null, null, 3.3);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", "b");
    res = (Boolean) checkLtOp.invoke(service, cond, "c", null, null);
    assertThat(res).isFalse();

    cond = new SingleFilterCondition(ImmutableList.of("a"), "$lt", "A");
    res = (Boolean) checkLtOp.invoke(service, cond, null, true, null);
    assertThat(res).isNull();
  }

  public static List<Row> makeInitialRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data0 = new HashMap<>();
    Map<String, Object> data1 = new HashMap<>();
    Map<String, Object> data2 = new HashMap<>();
    Map<String, Object> data3 = new HashMap<>();
    data0.put("key", "1");
    data1.put("key", "1");
    data2.put("key", "1");
    data3.put("key", "1");

    data0.put("writetime(leaf)", 0L);
    data1.put("writetime(leaf)", 0L);
    data2.put("writetime(leaf)", 0L);
    data3.put("writetime(leaf)", 0L);

    data0.put("p0", "");
    data0.put("p1", "");
    data0.put("p2", "");
    data0.put("p3", "");
    data0.put("leaf", DocumentDB.ROOT_DOC_MARKER);

    data1.put("p0", "a");
    data1.put("p1", "b");
    data1.put("p2", "c");
    data1.put("bool_value", true);
    data1.put("p3", "");
    data1.put("leaf", "c");

    data2.put("p0", "d");
    data2.put("p1", "e");
    data2.put("p2", "[0]");
    data2.put("dbl_value", 3.0);
    data2.put("p3", "");
    data2.put("leaf", "[0]");

    data3.put("p0", "f");
    data3.put("text_value", "abc");
    data3.put("p1", "");
    data3.put("p2", "");
    data3.put("p3", "");
    data3.put("leaf", "f");

    rows.add(makeRow(data0, numericBooleans));
    rows.add(makeRow(data1, numericBooleans));
    rows.add(makeRow(data2, numericBooleans));
    rows.add(makeRow(data3, numericBooleans));

    return rows;
  }

  public static List<Row> makeSecondRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();
    data1.put("key", "1");
    data1.put("writetime(leaf)", 1L);
    data1.put("p0", "a");
    data1.put("p1", "b");
    data1.put("p2", "c");
    data1.put("p3", "d");
    data1.put("text_value", "replaced");
    data1.put("leaf", "d");
    data1.put("p4", "");
    rows.add(makeRow(data1, numericBooleans));

    Map<String, Object> data2 = new HashMap<>();
    data2.put("key", "1");
    data2.put("writetime(leaf)", 1L);
    data2.put("p0", "d");
    data2.put("p1", "e");
    data2.put("p2", "f");
    data2.put("p3", "g");
    data2.put("text_value", "replaced");
    data2.put("leaf", "g");
    data2.put("p4", "");
    rows.add(makeRow(data2, numericBooleans));
    return rows;
  }

  public static List<Row> makeThirdRowData(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();

    data1.put("key", "1");
    data1.put("writetime(leaf)", 2L);
    data1.put("p0", "[0]");
    data1.put("text_value", "replaced");
    data1.put("p1", "");
    data1.put("p2", "");
    data1.put("p3", "");
    data1.put("leaf", "[0]");
    ;

    rows.add(makeRow(data1, numericBooleans));
    return rows;
  }

  public static List<Row> makeMultipleReplacements() {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();

    data1.put("key", "1");
    data1.put("writetime(leaf)", 0L);
    data1.put("p0", "a");
    data1.put("p1", "[0]");
    data1.put("p2", "b");
    data1.put("p3", "c");
    data1.put("text_value", "initial");
    data1.put("p4", "");
    data1.put("leaf", "c");

    Map<String, Object> data2 = new HashMap<>();

    data2.put("key", "1");
    data2.put("writetime(leaf)", 1L);
    data2.put("p0", "a");
    data2.put("p1", "[0]");
    data2.put("p2", "");
    data2.put("p3", "");
    data2.put("text_value", "initial");
    data2.put("leaf", "a");

    Map<String, Object> data3 = new HashMap<>();

    data3.put("key", "1");
    data3.put("writetime(leaf)", 2L);
    data3.put("p0", "a");
    data3.put("p1", "[0]");
    data3.put("p2", "[0]");
    data3.put("dbl_value", 1.23);
    data3.put("p3", "");
    data3.put("leaf", "a");

    Map<String, Object> data4 = new HashMap<>();

    data4.put("key", "1");
    data4.put("writetime(leaf)", 3L);
    data4.put("p0", "a");
    data4.put("p1", "[0]");
    data4.put("p2", "c");
    data4.put("text_value", DocumentDB.EMPTY_ARRAY_MARKER);
    data4.put("p3", "");
    data4.put("leaf", "c");

    Map<String, Object> data5 = new HashMap<>();

    data5.put("key", "1");
    data5.put("writetime(leaf)", 4L);
    data5.put("p0", "a");
    data5.put("p1", "b");
    data5.put("p2", "c");
    data5.put("text_value", DocumentDB.EMPTY_OBJECT_MARKER);
    data5.put("p3", "");
    data5.put("leaf", "c");

    rows.add(makeRow(data1, false));
    rows.add(makeRow(data2, false));
    rows.add(makeRow(data3, false));
    rows.add(makeRow(data4, false));
    rows.add(makeRow(data5, false));
    return rows;
  }

  public List<Row> makeRowDataForSecondDoc(boolean numericBooleans) {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();

    data1.put("key", "2");
    data1.put("writetime(leaf)", 2L);
    data1.put("p0", "[0]");
    data1.put("text_value", "replaced");
    data1.put("p1", "");
    data1.put("p2", "");
    data1.put("p3", "");
    data1.put("leaf", "[0]");

    rows.add(makeRow(data1, numericBooleans));
    return rows;
  }

  private List<PathSegment> smallPath() {
    List<PathSegment> path = new ArrayList<>();
    path.add(
        new PathSegment() {
          @Override
          public String getPath() {
            return "abc";
          }

          @Override
          public MultivaluedMap<String, String> getMatrixParameters() {
            return null;
          }
        });
    path.add(
        new PathSegment() {
          @Override
          public String getPath() {
            return "[0]";
          }

          @Override
          public MultivaluedMap<String, String> getMatrixParameters() {
            return null;
          }
        });
    return path;
  }

  private static Row makeRow(Map<String, Object> data, boolean numericBooleans) {
    List<Column> columns = new ArrayList<>(DocumentDB.allColumns());
    columns.add(Column.create("writetime(leaf)", Type.Bigint));
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    ProtocolVersion version = ProtocolVersion.DEFAULT;
    if (numericBooleans) {
      columns.replaceAll(
          col -> {
            if (col.name().equals("bool_value")) {
              return Column.create("bool_value", Type.Tinyint);
            }
            return col;
          });
    }

    for (Column column : columns) {
      Object v = data.get(column.name());
      if (column.name().equals("bool_value") && numericBooleans && v != null) {
        v = ((Boolean) v) ? (byte) 1 : (byte) 0;
      }
      values.add(v == null ? null : column.type().codec().encode(v, version));
    }
    return new ArrayListBackedRow(columns, values, version);
  }
}
