package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
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
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.ListFilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.resources.Db;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferGson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DocumentService.class)
public class DocumentServiceTest {
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
  private Method checkEqualsOp;
  private Method checkInOp;
  private Method checkGtOp;
  private Method checkLtOp;
  private Method searchRows;
  private static final ObjectMapper mapper = new ObjectMapper();

  @Before
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
            long.class,
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
            "updateExistenceForMap", Map.class, Map.class, List.class, List.class);
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
            "applyInMemoryFilters", List.class, List.class, int.class);
    applyInMemoryFilters.setAccessible(true);
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
            Boolean.class,
            String.class);
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

    result = (String) leftPadTo6.invoke(service, "longer string");
    assertThat(result).isEqualTo("longer string");

    result = (String) leftPadTo6.invoke(service, "å∫ç∂´");
    assertThat(result).isEqualTo("0å∫ç∂´");
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
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": true}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_numberLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": 3}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      null,
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_stringLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": \"leaf\"}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      null,
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_emptyObjectLeaf()
      throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": {}}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_emptyArrayLeaf()
      throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": []}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      null,
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_nullLeaf() throws InvocationTargetException, IllegalAccessException {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"cool\": {\"document\": null}}";
    ImmutablePair<List<Object[]>, List<String>> shredResult =
        (ImmutablePair<List<Object[]>, List<String>>)
            shredPayload.invoke(
                service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false);
    List<Object[]> bindVariables = shredResult.left;
    List<String> topLevelKeys = shredResult.right;
    assertThat(bindVariables.size()).isEqualTo(1);
    Object[] vars = bindVariables.get(0);
    assertThat(vars.length).isEqualTo(70);
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
      null,
      111L
    };
    for (int i = 0; i < 70; i++) {
      assertThat(vars[i]).isEqualTo(expected[i]);
    }

    assertThat(topLevelKeys.size()).isEqualTo(1);
    assertThat(topLevelKeys.get(0)).isEqualTo("cool");
  }

  @Test
  public void shredPayload_invalidKeys() {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "{\"coo]\": {\"document\": null}}";
    Throwable thrown =
        catchThrowable(
            () ->
                shredPayload.invoke(
                    service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, false));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("are not permitted in JSON field names, invalid field coo]");
    ;
  }

  @Test
  public void shredPayload_patchingArrayInvalid() {
    DocumentDB dbMock = mock(DocumentDB.class);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    List<String> path = new ArrayList<>();
    String key = "eric";
    String payload = "[1, 2, 3]";
    Throwable thrown =
        catchThrowable(
            () ->
                shredPayload.invoke(
                    service, JsonSurferGson.INSTANCE, dbMock, path, key, payload, 111L, true));
    assertThat(thrown.getCause())
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining("A patch operation must be done with a JSON object, not an array.");
    ;
  }

  @Test
  public void putAtRoot() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString())).thenReturn(dbMock);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();
    when(dbMock.insertBatchIfNotExists(anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(true);

    boolean success =
        service.putAtRoot(
            "authToken", "ks", "collection", "id", "{\"some\": \"data\"}", dbFactoryMock);

    assertThat(success).isTrue();
  }

  @Test
  public void putAtRoot_noData() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString())).thenReturn(dbMock);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();
    when(dbMock.insertBatchIfNotExists(anyString(), anyString(), anyString(), anyObject()))
        .thenReturn(true);

    Throwable thrown =
        catchThrowable(
            () -> service.putAtRoot("authToken", "ks", "collection", "id", "1", dbFactoryMock));

    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed.");
  }

  @Test
  public void putAtPath() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString())).thenReturn(dbMock);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    service.putAtPath(
        "authToken",
        "ks",
        "collection",
        "id",
        "{\"some\": \"data\"}",
        new ArrayList<>(),
        false,
        dbFactoryMock);

    verify(dbMock, times(1))
        .deleteThenInsertBatch(
            anyString(), anyString(), anyString(), anyObject(), anyObject(), anyLong());
    verify(dbMock, times(0))
        .deletePatchedPathsThenInsertBatch(
            anyString(),
            anyString(),
            anyString(),
            anyObject(),
            anyObject(),
            anyObject(),
            anyLong());
  }

  @Test
  public void putAtPath_patching() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString())).thenReturn(dbMock);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

    service.putAtPath(
        "authToken",
        "ks",
        "collection",
        "id",
        "{\"some\": \"data\"}",
        new ArrayList<>(),
        true,
        dbFactoryMock);

    verify(dbMock, times(0))
        .deleteThenInsertBatch(
            anyString(), anyString(), anyString(), anyObject(), anyObject(), anyLong());
    verify(dbMock, times(1))
        .deletePatchedPathsThenInsertBatch(
            anyString(),
            anyString(),
            anyString(),
            anyObject(),
            anyObject(),
            anyObject(),
            anyLong());
  }

  @Test
  public void putAtPath_noData() throws UnauthorizedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    Db dbFactoryMock = mock(Db.class);
    when(dbFactoryMock.getDocDataStoreForToken(anyString())).thenReturn(dbMock);
    when(dbMock.newBindMap(anyObject(), anyLong())).thenCallRealMethod();

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
                    dbFactoryMock));

    assertThat(thrown)
        .isInstanceOf(DocumentAPIRequestException.class)
        .hasMessageContaining(
            "Updating a key with just a JSON primitive, empty object, or empty array is not allowed.");
  }

  @Test
  public void getJsonAtPath() throws ExecutionException, InterruptedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    when(dbMock.executeSelect(anyString(), anyString(), anyList())).thenReturn(rsMock);
    when(rsMock.rows()).thenReturn(new ArrayList<>());

    List<PathSegment> path = smallPath();
    JsonNode result = service.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRowsEmptyJson() throws ExecutionException, InterruptedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyList())).thenReturn(rsMock);

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    when(serviceMock.convertToJsonDoc(anyList(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.createObjectNode(), new HashMap<>()));

    JsonNode result = serviceMock.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isNull();
  }

  @Test
  public void getJsonAtPath_withRows()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyList())).thenReturn(rsMock);

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    ObjectNode jsonObj = mapper.createObjectNode();
    jsonObj.set("abc", mapper.readTree("[1]"));

    when(serviceMock.convertToJsonDoc(anyList(), anyBoolean()))
        .thenReturn(ImmutablePair.of(jsonObj, new HashMap<>()));

    JsonNode result = serviceMock.getJsonAtPath(dbMock, "ks", "collection", "id", path);

    assertThat(result).isEqualTo(IntNode.valueOf(1));
  }

  @Test
  public void getJsonAtPath_withDeadLeaves()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    DocumentService serviceMock = mock(DocumentService.class, CALLS_REAL_METHODS);
    when(dbMock.executeSelect(anyString(), anyString(), anyList())).thenReturn(rsMock);

    List<Row> rows = makeInitialRowData();
    when(rsMock.rows()).thenReturn(rows);

    List<PathSegment> path = smallPath();

    ObjectNode jsonObj = mapper.createObjectNode();
    jsonObj.set("abc", mapper.readTree("[1]"));

    Map<String, List<JsonNode>> deadLeaves = new HashMap<>();
    deadLeaves.put("a", new ArrayList<>());
    when(serviceMock.convertToJsonDoc(anyList(), anyBoolean()))
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
  public void deleteAtPath() {
    DocumentDB dbMock = mock(DocumentDB.class);
    service.deleteAtPath(dbMock, "keyspace", "collection", "id", smallPath());
    verify(dbMock, times(1)).delete(anyString(), anyString(), anyString(), anyList(), anyLong());
  }

  // searchDocuments unit tests excluded here, it is in deprecated v1

  @Test
  public void searchDocumentsV2_emptyResult() throws Exception {
    DocumentDB dbMock = PowerMockito.mock(DocumentDB.class);
    DocumentService serviceMock = PowerMockito.mock(DocumentService.class);
    PowerMockito.when(
            serviceMock.searchDocumentsV2(
                anyObject(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenCallRealMethod();
    PowerMockito.when(
            serviceMock,
            "searchRows",
            anyString(),
            anyString(),
            anyObject(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            anyString())
        .thenReturn(ImmutablePair.of(new ArrayList<>(), null));

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    ImmutablePair<JsonNode, ByteBuffer> result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, new ArrayList<>(), null);
    assertThat(result).isNull();
  }

  @Test
  public void searchDocumentsV2_existingResult() throws Exception {
    DocumentDB dbMock = PowerMockito.mock(DocumentDB.class);
    DocumentService serviceMock = PowerMockito.mock(DocumentService.class);
    PowerMockito.when(
            serviceMock.searchDocumentsV2(
                anyObject(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenCallRealMethod();
    PowerMockito.when(
            serviceMock,
            "searchRows",
            anyString(),
            anyString(),
            anyObject(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            anyString())
        .thenReturn(ImmutablePair.of(makeInitialRowData(), null));
    PowerMockito.when(serviceMock.convertToJsonDoc(anyObject(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$eq", "value"));
    ImmutablePair<JsonNode, ByteBuffer> result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, new ArrayList<>(), null);
    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(mapper.readTree("{\"1\":[{\"a\":1},{\"a\":1},{\"a\":1}]}"));
  }

  @Test
  public void searchDocumentsV2_existingResultWithFields() throws Exception {
    DocumentDB dbMock = PowerMockito.mock(DocumentDB.class);
    DocumentService serviceMock = PowerMockito.mock(DocumentService.class);
    PowerMockito.when(
            serviceMock.searchDocumentsV2(
                anyObject(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenCallRealMethod();
    PowerMockito.when(
            serviceMock,
            "searchRows",
            anyString(),
            anyString(),
            anyObject(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            anyString())
        .thenReturn(ImmutablePair.of(makeInitialRowData(), null));
    PowerMockito.when(serviceMock.convertToJsonDoc(anyObject(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));

    List<FilterCondition> filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$exists", true));
    ImmutablePair<JsonNode, ByteBuffer> result =
        serviceMock.searchDocumentsV2(
            dbMock, "keyspace", "collection", filters, ImmutableList.of("field"), null);
    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(mapper.readTree("{\"1\":[{\"a\":1},{\"a\":1},{\"a\":1}]}"));
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
    Map<String, Boolean> existenceByDoc = new HashMap<>();
    Map<String, Integer> countsByDoc = new HashMap<>();
    List<Row> rows = makeInitialRowData();
    updateExistenceForMap.invoke(service, existenceByDoc, countsByDoc, rows, new ArrayList<>());
    assertThat(existenceByDoc.get("1")).isTrue();
    assertThat(countsByDoc.get("1")).isEqualTo(3);
  }

  @Test
  public void getFullDocuments_lessThanLimit() throws Exception {
    Db dbFactoryMock = PowerMockito.mock(Db.class);
    DocumentDB dbMock = PowerMockito.mock(DocumentDB.class);
    DocumentService serviceMock = PowerMockito.mock(DocumentService.class);
    PowerMockito.when(dbFactoryMock.getDocDataStoreForToken(anyString(), anyInt(), anyObject()))
        .thenReturn(dbMock);
    PowerMockito.when(
            serviceMock,
            "searchRows",
            anyString(),
            anyString(),
            anyObject(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            anyString())
        .thenReturn(ImmutablePair.of(makeInitialRowData(), null));
    PowerMockito.when(
            serviceMock.getFullDocuments(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenCallRealMethod();
    PowerMockito.when(serviceMock, "addRowsToMap", anyMap(), anyList()).thenCallRealMethod();
    PowerMockito.when(serviceMock.convertToJsonDoc(anyObject(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));

    ImmutablePair<JsonNode, ByteBuffer> result =
        serviceMock.getFullDocuments(
            dbFactoryMock,
            dbMock,
            "authToken",
            "keyspace",
            "collection",
            new ArrayList<>(),
            null,
            100,
            1);
    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void getFullDocuments_greaterThanLimit() throws Exception {
    Db dbFactoryMock = PowerMockito.mock(Db.class);
    DocumentDB dbMock = PowerMockito.mock(DocumentDB.class);
    DocumentService serviceMock = PowerMockito.mock(DocumentService.class);
    List<Row> twoDocsRows = makeInitialRowData();
    twoDocsRows.addAll(makeRowDataForSecondDoc());
    PowerMockito.when(dbFactoryMock.getDocDataStoreForToken(anyString(), anyInt(), anyObject()))
        .thenReturn(dbMock);
    PowerMockito.when(
            serviceMock,
            "searchRows",
            anyString(),
            anyString(),
            anyObject(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            anyString())
        .thenReturn(ImmutablePair.of(twoDocsRows, null));
    PowerMockito.when(
            serviceMock.getFullDocuments(
                anyObject(),
                anyObject(),
                anyString(),
                anyString(),
                anyString(),
                anyList(),
                anyObject(),
                anyInt(),
                anyInt()))
        .thenCallRealMethod();
    PowerMockito.when(serviceMock, "addRowsToMap", anyMap(), anyList()).thenCallRealMethod();
    PowerMockito.when(serviceMock.convertToJsonDoc(anyObject(), anyBoolean()))
        .thenReturn(ImmutablePair.of(mapper.readTree("{\"a\": 1}"), new HashMap<>()));

    ImmutablePair<JsonNode, ByteBuffer> result =
        serviceMock.getFullDocuments(
            dbFactoryMock,
            dbMock,
            "authToken",
            "keyspace",
            "collection",
            new ArrayList<>(),
            null,
            100,
            1);
    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(mapper.readTree("{\"1\": {\"a\": 1}}"));
  }

  @Test
  public void searchRows()
      throws InvocationTargetException, IllegalAccessException, ExecutionException,
          InterruptedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData();
    when(dbMock.executeSelectAll(anyString(), anyString())).thenReturn(rsMock);
    when(dbMock.executeSelect(anyString(), anyString(), anyObject(), anyBoolean()))
        .thenReturn(rsMock);
    when(rsMock.rows()).thenReturn(rows);

    List<FilterCondition> filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a,b", "*", "c"), "$eq", true));

    ImmutablePair<List<Row>, ByteBuffer> result =
        (ImmutablePair<List<Row>, ByteBuffer>)
            searchRows.invoke(
                service,
                "keyspace",
                "collection",
                dbMock,
                filters,
                new ArrayList<>(),
                ImmutableList.of("a,b", "*", "c"),
                null,
                null);

    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(rows);

    result =
        (ImmutablePair<List<Row>, ByteBuffer>)
            searchRows.invoke(
                service,
                "keyspace",
                "collection",
                dbMock,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                null,
                null);

    assertThat(result.right).isNull();
    assertThat(result.left).isEqualTo(rows);
  }

  @Test
  public void searchRows_invalid() throws ExecutionException, InterruptedException {
    DocumentDB dbMock = mock(DocumentDB.class);
    ResultSet rsMock = mock(ResultSet.class);
    List<Row> rows = makeInitialRowData();
    when(dbMock.executeSelectAll(anyString(), anyString())).thenReturn(rsMock);
    when(dbMock.executeSelect(anyString(), anyString(), anyObject(), anyBoolean()))
        .thenReturn(rsMock);
    when(rsMock.rows()).thenReturn(rows);
    when(rsMock.getPagingState()).thenReturn(ByteBuffer.wrap(new byte[0]));

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
                    filters,
                    new ArrayList<>(),
                    ImmutableList.of("a,b", "*", "c"),
                    null,
                    null));

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
    List<Row> result =
        (List<Row>)
            filterToSelectionSet.invoke(
                service, rows, new ArrayList<>(), ImmutableList.of("a", "b"));
    assertThat(result).isEqualTo(rows);

    List<String> selectionSet = ImmutableList.of("c", "n1", "n2", "n3");
    result =
        (List<Row>)
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
    List<Row> result = (List<Row>) applyInMemoryFilters.invoke(service, rows, new ArrayList<>(), 1);
    assertThat(result).isEqualTo(rows);

    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isEqualTo(rows.get(0));

    filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$exists", true));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0)).isEqualTo(rows.get(0));

    filters =
        ImmutableList.of(
            new SingleFilterCondition(ImmutableList.of("a", "b", "x"), "$exists", true));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$gt", true));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$gte", 2.0));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lt", true));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$lte", false));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(new SingleFilterCondition(ImmutableList.of("a", "b", "c"), "$ne", true));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(
            new ListFilterCondition(
                ImmutableList.of("a", "b", "c"), "$in", ImmutableList.of(false)));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);

    filters =
        ImmutableList.of(
            new ListFilterCondition(
                ImmutableList.of("a", "b", "c"), "$nin", ImmutableList.of(true)));
    result = (List<Row>) applyInMemoryFilters.invoke(service, rows, filters, 1);
    assertThat(result.size()).isEqualTo(0);
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
        (row1, row2) -> {
          return (row1.getString(Column.reference("p0"))
                      .compareTo(row2.getString(Column.reference("p0")))
                  * 100000
              + row1.getString(Column.reference("p1"))
                      .compareTo(row2.getString(Column.reference("p1")))
                  * 10000
              + row1.getString(Column.reference("p2"))
                      .compareTo(row2.getString(Column.reference("p2")))
                  * 1000
              + row1.getString(Column.reference("p3"))
                      .compareTo(row2.getString(Column.reference("p3")))
                  * 100);
        });
    ImmutablePair<JsonNode, Map<String, List<JsonNode>>> result =
        service.convertToJsonDoc(initial, false);

    assertThat(result.left.toString())
        .isEqualTo(
            mapper
                .readTree("{\"a\": {\"b\": {\"c\": true}}, \"d\": {\"e\": [3]}, \"f\": \"abc\"}")
                .toString());

    // This state should have no dead leaves, as it's the initial write
    assertThat(result.right).isEmpty();

    initial.addAll(makeSecondRowData());
    initial.sort(
        (row1, row2) -> {
          return (row1.getString("p0").compareTo(row2.getString("p0")) * 100000
              + row1.getString("p1").compareTo(row2.getString("p1")) * 10000
              + row1.getString("p2").compareTo(row2.getString("p2")) * 1000
              + row1.getString("p3").compareTo(row2.getString("p3")) * 100);
        });
    result = service.convertToJsonDoc(initial, false);

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
        (row1, row2) -> {
          return (row1.getString("p0").compareTo(row2.getString("p0")) * 100000
              + row1.getString("p1").compareTo(row2.getString("p1")) * 10000
              + row1.getString("p2").compareTo(row2.getString("p2")) * 1000
              + row1.getString("p3").compareTo(row2.getString("p3")) * 100);
        });
    result = service.convertToJsonDoc(initial, false);

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
    Map<Column, Object> data1 = new HashMap<>();
    Map<Column, Object> data2 = new HashMap<>();
    Map<Column, Object> data3 = new HashMap<>();
    data1.put(Column.reference("key"), "1");
    data2.put(Column.reference("key"), "1");
    data3.put(Column.reference("key"), "1");

    data1.put(Column.reference("writetime(leaf)"), 0L);
    data2.put(Column.reference("writetime(leaf)"), 0L);
    data3.put(Column.reference("writetime(leaf)"), 0L);

    data1.put(Column.reference("p0"), "a");
    data1.put(Column.reference("p1"), "b");
    data1.put(Column.reference("p2"), "c");
    data1.put(Column.reference("bool_value"), true);
    data1.put(Column.reference("p3"), "");
    data1.put(Column.reference("leaf"), "c");

    data2.put(Column.reference("p0"), "d");
    data2.put(Column.reference("p1"), "e");
    data2.put(Column.reference("p2"), "[0]");
    data2.put(Column.reference("dbl_value"), 3.0);
    data2.put(Column.reference("p3"), "");
    data2.put(Column.reference("leaf"), "[0]");

    data3.put(Column.reference("p0"), "f");
    data3.put(Column.reference("text_value"), "abc");
    data3.put(Column.reference("p1"), "");
    data3.put(Column.reference("p2"), "");
    data3.put(Column.reference("p3"), "");
    data3.put(Column.reference("leaf"), "f");

    rows.add(new TestRow(data1));
    rows.add(new TestRow(data2));
    rows.add(new TestRow(data3));

    return rows;
  }

  private List<Row> makeSecondRowData() {
    List<Row> rows = new ArrayList<>();
    Map<Column, Object> data1 = new HashMap<>();
    data1.put(Column.reference("key"), "1");
    data1.put(Column.reference("writetime(leaf)"), 1L);
    data1.put(Column.reference("p0"), "a");
    data1.put(Column.reference("p1"), "b");
    data1.put(Column.reference("p2"), "c");
    data1.put(Column.reference("p3"), "d");
    data1.put(Column.reference("text_value"), "replaced");
    data1.put(Column.reference("leaf"), "d");
    data1.put(Column.reference("p4"), "");
    rows.add(new TestRow(data1));
    return rows;
  }

  private List<Row> makeThirdRowData() {
    List<Row> rows = new ArrayList<>();
    Map<Column, Object> data1 = new HashMap<>();

    data1.put(Column.reference("key"), "1");
    data1.put(Column.reference("writetime(leaf)"), 2L);
    data1.put(Column.reference("p0"), "[0]");
    data1.put(Column.reference("text_value"), "replaced");
    data1.put(Column.reference("p1"), "");
    data1.put(Column.reference("p2"), "");
    data1.put(Column.reference("p3"), "");
    data1.put(Column.reference("leaf"), "[0]");

    rows.add(new TestRow(data1));
    return rows;
  }

  private List<Row> makeRowDataForSecondDoc() {
    List<Row> rows = new ArrayList<>();
    Map<Column, Object> data1 = new HashMap<>();

    data1.put(Column.reference("key"), "2");
    data1.put(Column.reference("writetime(leaf)"), 2L);
    data1.put(Column.reference("p0"), "[0]");
    data1.put(Column.reference("text_value"), "replaced");
    data1.put(Column.reference("p1"), "");
    data1.put(Column.reference("p2"), "");
    data1.put(Column.reference("p3"), "");
    data1.put(Column.reference("leaf"), "[0]");

    rows.add(new TestRow(data1));
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

  private class TestTable extends AbstractTable {
    public List<Column> columns() {
      return DocumentDB.allColumns();
    }

    public int priority() {
      return 0;
    }

    public String indexTypeName() {
      return "";
    }

    public String name() {
      return "collection";
    }

    public String keyspace() {
      return "eric";
    }
  }

  private class TestRow implements Row {
    private Map<Column, Object> data;

    public TestRow(Map<Column, Object> data) {
      this.data = data;
    }

    public boolean has(String column) {
      return data.containsKey(Column.reference(column));
    }

    public List<Column> columns() {
      return DocumentDB.allColumns();
    }

    public int getInt(Column c) {
      return (int) data.get(c);
    }

    public long getLong(Column c) {
      return (long) data.get(c);
    }

    public String getString(Column c) {
      return (String) data.get(c);
    }

    public boolean getBoolean(Column c) {
      return (boolean) data.get(c);
    }

    public byte getByte(Column c) {
      return (byte) data.get(c);
    }

    public short getShort(Column c) {
      return (short) data.get(c);
    }

    public double getDouble(Column c) {
      return (double) data.get(c);
    }

    public float getFloat(Column c) {
      return (float) data.get(c);
    }

    public ByteBuffer getBytes(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public InetAddress getInetAddress(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public BigInteger getVarint(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public UUID getUUID(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public BigDecimal getDecimal(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public Instant getTimestamp(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public LocalTime getTime(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public LocalDate getDate(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public long getCounter(Column c) {
      throw new NotImplementedException("Not Implemented");
    }

    public CqlDuration getDuration(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public <T> List<T> getList(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public <T> Set<T> getSet(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public <K, V> Map<K, V> getMap(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public TupleValue getTuple(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public UdtValue getUDT(Column column) {
      throw new NotImplementedException("Not Implemented");
    }

    public AbstractTable table() {
      return new TestTable();
    }
  }
}
