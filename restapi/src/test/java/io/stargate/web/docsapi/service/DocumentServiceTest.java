package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferGson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DocumentServiceTest {
  private final DataStore dataStore = Mockito.mock(DataStore.class);
  private DocumentService service;
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
    service = new DocumentService();

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
    searchRows =
        DocumentService.class.getDeclaredMethod(
            "searchRows",
            String.class,
            String.class,
            DocumentDB.class,
            List.class,
            List.class,
            List.class,
            List.class,
            Boolean.class,
            String.class,
            Paginator.class);
    searchRows.setAccessible(true);
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
    when(dbMock.newBindMap(any())).thenCallRealMethod();

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
  public void getJsonAtPath()
      throws ExecutionException, InterruptedException, UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);
    when(rsMock.rows()).thenReturn(new ArrayList<>());
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    List<PathSegment> path = smallPath();
    JsonNode result = service.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRowsEmptyJson()
      throws ExecutionException, InterruptedException, UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    when(serviceMock.convertToJsonDoc(anyListOf(Row.class), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.createObjectNode(), new HashMap<>()));

    JsonNode result = serviceMock.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRows()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    ObjectNode jsonObj = mapper.createObjectNode();
    jsonObj.set("abc", mapper.readTree("[1]"));

    when(serviceMock.convertToJsonDoc(anyListOf(Row.class), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(jsonObj, new HashMap<>()));

    JsonNode result = serviceMock.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isEqualTo(IntNode.valueOf(1));
  }

  @Test
  public void getJsonAtPath_withDeadLeaves()
      throws ExecutionException, InterruptedException, JsonProcessingException,
          UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyListOf(BuiltCondition.class)))
        .thenReturn(rsMock);
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    ObjectNode jsonObj = mapper.createObjectNode();
    jsonObj.set("abc", mapper.readTree("[1]"));

    Map<String, List<JsonNode>> deadLeaves = new HashMap<>();
    deadLeaves.put("a", new ArrayList<>());
    when(serviceMock.convertToJsonDoc(anyListOf(Row.class), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(jsonObj, deadLeaves));

    JsonNode result = serviceMock.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isEqualTo(IntNode.valueOf(1));
    verify(dbMock, times(1)).deleteDeadLeaves("ks", "collection", "id", deadLeaves);
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
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    service.deleteAtPath(dbMock, "keyspace", "collection", "id", smallPath());
    verify(dbMock, times(1))
        .delete(anyString(), anyString(), anyString(), anyListOf(String.class), anyLong());
  }

  // searchDocuments unit tests excluded here, it is in deprecated v1

  @Test
  public void searchDocumentsV2_emptyResult() throws Exception {
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    Mockito.when(serviceMock.searchDocumentsV2(any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();
    Mockito.when(
            serviceMock.searchRows(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(new ArrayList<>());
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    JsonNode result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, new ArrayList<>(), null, paginator);
    assertThat(result).isNull();
  }

  @Test
  public void searchDocumentsV2_existingResult() throws Exception {
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    Mockito.when(serviceMock.searchDocumentsV2(any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();
    Mockito.when(
            serviceMock.searchRows(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(makeInitialRowData());
    Mockito.when(serviceMock.convertToJsonDoc(any(), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    JsonNode result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, new ArrayList<>(), null, paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\":[{\"a\":1},{\"a\":1},{\"a\":1}]}"));
  }

  @Test
  public void searchDocumentsV2_existingResultWithFields() throws Exception {
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    Mockito.when(serviceMock.searchDocumentsV2(any(), any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();
    Mockito.when(
            serviceMock.searchRows(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(makeInitialRowData());
    Mockito.when(serviceMock.convertToJsonDoc(any(), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));
    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$exists", true));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);
    JsonNode result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, ImmutableList.of("field"), null, paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\":[{\"a\":1},{\"a\":1},{\"a\":1}]}"));
  }

  @Test
  public void addRowsToMap() throws InvocationTargetException, IllegalAccessException {
    Map<String, List<Row>> rowsByDoc = new HashMap<>();
    List<Row> rows = makeInitialRowData();
    addRowsToMap.invoke(service, rowsByDoc, rows);
    assertThat(rowsByDoc.get("1")).isEqualTo(rows);
  }

  @Test
  public void updateExistenceForMap() throws InvocationTargetException, IllegalAccessException {
    Set<String> existenceByDoc = new HashSet<>();
    List<Row> rows = makeInitialRowData();
    updateExistenceForMap.invoke(service, existenceByDoc, rows, new ArrayList<>(), false, true);
    assertThat(existenceByDoc.contains("1")).isTrue();
  }

  @Test
  public void getFullDocuments_lessThanLimit() throws Exception {
    Db dbFactoryMock = Mockito.mock(Db.class);
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    Mockito.when(dbFactoryMock.getDocDataStoreForToken(anyString(), any())).thenReturn(dbMock);
    Mockito.when(
            serviceMock.searchRows(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(makeInitialRowData());
    Mockito.when(
            serviceMock.getFullDocuments(
                any(), anyString(), anyString(), anyListOf(String.class), any()))
        .thenCallRealMethod();
    Mockito.doCallRealMethod().when(serviceMock).addRowsToMap(anyMap(), anyList());
    Mockito.when(serviceMock.convertToJsonDoc(any(), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    JsonNode result =
        serviceMock.getFullDocuments(
            dbMock, "keyspace", "collection", new ArrayList<>(), paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void getFullDocuments_greaterThanLimit() throws Exception {
    Db dbFactoryMock = Mockito.mock(Db.class);
    DocumentDB dbMock = Mockito.mock(DocumentDB.class);
    DocumentService serviceMock = Mockito.mock(DocumentService.class);
    List<Row> twoDocsRows = makeInitialRowData();
    twoDocsRows.addAll(makeRowDataForSecondDoc());
    Mockito.when(dbFactoryMock.getDocDataStoreForToken(anyString(), any())).thenReturn(dbMock);
    Mockito.when(
            serviceMock.searchRows(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(twoDocsRows);
    Mockito.when(
            serviceMock.getFullDocuments(
                any(), anyString(), anyString(), anyListOf(String.class), any()))
        .thenCallRealMethod();
    Mockito.doCallRealMethod().when(serviceMock).addRowsToMap(anyMap(), anyList());
    Mockito.when(serviceMock.convertToJsonDoc(any(), anyBoolean(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));
    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    JsonNode result =
        serviceMock.getFullDocuments(
            dbMock, "keyspace", "collection", new ArrayList<>(), paginator);
    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void searchRows()
      throws InvocationTargetException, IOException, IllegalAccessException, UnauthorizedException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);

    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData();
    when(dbMock.executeSelectAll(anyString(), anyString(), anyInt(), any())).thenReturn(rsMock);
    when(dbMock.executeSelect(anyString(), anyString(), any(), anyBoolean(), anyInt(), any()))
        .thenReturn(rsMock);
    when(rsMock.currentPageRows()).thenReturn(rows);
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));

    List<FilterCondition> filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a,b", "*", "c"), "$eq", true));

    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<Row> result =
        (List<Row>)
            searchRows.invoke(
                service,
                "keyspace",
                "collection",
                dbMock,
                new ArrayList<>(),
                filters,
                new ArrayList<>(),
                ImmutableList.of("a,b", "*", "c"),
                false,
                null,
                paginator);

    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(rows);

    paginator = new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);
    result =
        (List<Row>)
            searchRows.invoke(
                service,
                "keyspace",
                "collection",
                dbMock,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                false,
                null,
                paginator);

    assertThat(paginator.getCurrentDbPageState()).isNull();
    assertThat(result).isEqualTo(rows);
  }

  @Test
  public void searchRows_invalid() throws UnauthorizedException, IOException {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData();
    when(dbMock.executeSelectAll(anyString(), anyString(), anyInt(), any())).thenReturn(rsMock);
    when(dbMock.executeSelect(anyString(), anyString(), any(), anyBoolean(), anyInt(), any()))
        .thenReturn(rsMock);
    when(dbMock.getAuthorizationService()).thenReturn(authorizationService);
    doNothing()
        .when(authorizationService)
        .authorizeDataRead(
            any(AuthenticationSubject.class), anyString(), anyString(), eq(SourceAPI.REST));
    when(rsMock.rows()).thenReturn(rows);
    when(rsMock.getPagingState()).thenReturn(ByteBuffer.wrap(new byte[0]));

    int pageSizeParam = 0;
    Paginator paginator =
        new Paginator(dataStore, null, pageSizeParam, DocumentDB.SEARCH_PAGE_SIZE);

    List<FilterCondition> filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a,b", "*", "c"), "$ne", true));

    Throwable thrown =
        catchThrowable(
            () ->
                searchRows.invoke(
                    service,
                    "keyspace",
                    "collection",
                    dbMock,
                    new ArrayList<>(),
                    filters,
                    new ArrayList<>(),
                    ImmutableList.of("a,b", "*", "c"),
                    null,
                    null,
                    paginator));

    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessage(
            "The results as requested must fit in one page, try increasing the `page-size` parameter.");
  }

  @Test
  public void getParentPathFromRow() throws InvocationTargetException, IllegalAccessException {
    Row row = makeInitialRowData().get(0);
    String result = (String) getParentPathFromRow.invoke(service, row);
    assertThat(result).isEqualTo("1/a.b.");
  }

  @Test
  public void filterToSelectionSet() throws InvocationTargetException, IllegalAccessException {
    List<Row> rows = makeInitialRowData();
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
    List<Row> rows = makeInitialRowData();
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

  @Test
  public void convertToJsonDoc_testDeadLeaves() throws JsonProcessingException {
    List<Row> initial = makeInitialRowData();
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    ImmutablePair<JsonNode, Map<String, List<JsonNode>>> result =
        service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString())
        .isEqualTo(
            mapper
                .readTree("{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(result.right).isEmpty();

    initial.addAll(makeSecondRowData());
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString())
        .isEqualTo(
            mapper
                .readTree(
                    "{\"a\": {\"b\": {\"c\": {\"d\": \"replaced\"}}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have 1 dead leaf, since $.a.b.c was changed from `true` to an object
    Map<String, List<JsonNode>> expected = new HashMap<>();
    List<JsonNode> list = new ArrayList<>();
    ObjectNode node = mapper.createObjectNode();
    node.set("", BooleanNode.valueOf(true));
    list.add(node);
    expected.put("$.a.b.c", list);
    assertThat(result.right).isEqualTo(expected);

    initial.addAll(makeThirdRowData());
    initial.sort(
        (row1, row2) ->
            (Objects.requireNonNull(row1.getString("p0"))
                        .compareTo(Objects.requireNonNull(row2.getString("p0")))
                    * 100000
                + Objects.requireNonNull(row1.getString("p1"))
                        .compareTo(Objects.requireNonNull(row2.getString("p1")))
                    * 10000
                + Objects.requireNonNull(row1.getString("p2"))
                        .compareTo(Objects.requireNonNull(row2.getString("p2")))
                    * 1000
                + Objects.requireNonNull(row1.getString("p3"))
                        .compareTo(Objects.requireNonNull(row2.getString("p3")))
                    * 100));
    result = service.convertToJsonDoc(initial, false, false);

    assertThat(result.left.toString()).isEqualTo(mapper.readTree("[\"replaced\"]").toString());

    // This state should have 3 dead branches representing keys a, d, and f, since everything was
    // blown away by the latest change
    expected = new HashMap<>();
    list = new ArrayList<>();
    node = mapper.createObjectNode();
    node.set("a", NullNode.getInstance());
    node.set("d", NullNode.getInstance());
    node.set("f", NullNode.getInstance());

    list.add(node);
    expected.put("$", list);

    assertThat(result.right).isEqualTo(expected);
  }

  private List<Row> makeInitialRowData() {
    List<Row> rows = new ArrayList<>();
    Map<String, Object> data1 = new HashMap<>();
    Map<String, Object> data2 = new HashMap<>();
    Map<String, Object> data3 = new HashMap<>();
    data1.put("key", "1");
    data2.put("key", "1");
    data3.put("key", "1");

    data1.put("writetime(leaf)", 0L);
    data2.put("writetime(leaf)", 0L);
    data3.put("writetime(leaf)", 0L);

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

    rows.add(makeRow(data1));
    rows.add(makeRow(data2));
    rows.add(makeRow(data3));

    return rows;
  }

  private List<Row> makeSecondRowData() {
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
    rows.add(makeRow(data1));
    return rows;
  }

  private List<Row> makeThirdRowData() {
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

    rows.add(makeRow(data1));
    return rows;
  }

  private List<Row> makeRowDataForSecondDoc() {
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

    rows.add(makeRow(data1));
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

  private static Row makeRow(Map<String, Object> data) {
    List<Column> columns = new ArrayList<>(DocumentDB.allColumns());
    columns.add(Column.create("writetime(leaf)", Type.Bigint));
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    ProtocolVersion version = ProtocolVersion.DEFAULT;
    for (Column column : columns) {
      Object v = data.get(column.name());
      values.add(v == null ? null : column.type().codec().encode(v, version));
    }
    return new ArrayListBackedRow(columns, values, version);
  }
}
