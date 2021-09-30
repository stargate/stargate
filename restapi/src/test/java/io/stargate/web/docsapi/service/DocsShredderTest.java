package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DocsShredderTest {
  private DocsShredder service;
  private final JsonSurfer surfer = JsonSurferJackson.INSTANCE;
  @Mock private DocumentDB db;

  @BeforeEach
  public void setup() {
    service = new DocsShredder(DocsApiConfiguration.DEFAULT);
  }

  public boolean arraysMatch(Object[] array1, Object[] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (array1[i] != array2[i] && !array1[i].equals(array2[i])) {
        System.out.println(array1[i] + " is not " + array2[i]);
        return false;
      }
    }
    return true;
  }

  @Test
  public void shredJson_happyPath() {
    List<Object[]> expectedRows = getExpectedRows();
    String payload =
        "{\"some\":\"value\", \"array\":[1, true, 3], \"nested\": {\"structure\": 23.456}}";
    ImmutablePair<List<Object[]>, List<String>> results =
        service.shredJson(surfer, Collections.emptyList(), "documentId", payload, false, false);
    List<Object[]> bindParams = results.left;
    assertThat(bindParams.size()).isEqualTo(expectedRows.size());
    assertThat(arraysMatch(bindParams.get(0), expectedRows.get(0))).isTrue();
    assertThat(arraysMatch(bindParams.get(1), expectedRows.get(1))).isTrue();
    assertThat(arraysMatch(bindParams.get(2), expectedRows.get(2))).isTrue();
    assertThat(arraysMatch(bindParams.get(3), expectedRows.get(3))).isTrue();
    assertThat(arraysMatch(bindParams.get(4), expectedRows.get(4))).isTrue();

    Set<String> firstLevelKeyset = new HashSet<>(results.right);
    assertThat(firstLevelKeyset.size()).isEqualTo(3);
    assertThat(firstLevelKeyset).contains("some", "array", "nested");
  }

  @Test
  public void shredJson_happyPathAtNestedPath() {
    List<Object[]> expectedRows = getNestedExpectedRows();
    List<String> prependedPath = ImmutableList.of("nested", "path", "spot");
    String payload =
        "{\"some\":\"value\", \"array\":[1, true, 3], \"nested\": {\"structure\": 23.456}}";
    ImmutablePair<List<Object[]>, List<String>> results =
        service.shredJson(surfer, prependedPath, "documentId", payload, false, false);
    List<Object[]> bindParams = results.left;
    assertThat(bindParams.size()).isEqualTo(expectedRows.size());
    assertThat(arraysMatch(bindParams.get(0), expectedRows.get(0))).isTrue();
    assertThat(arraysMatch(bindParams.get(1), expectedRows.get(1))).isTrue();
    assertThat(arraysMatch(bindParams.get(2), expectedRows.get(2))).isTrue();
    assertThat(arraysMatch(bindParams.get(3), expectedRows.get(3))).isTrue();
    assertThat(arraysMatch(bindParams.get(4), expectedRows.get(4))).isTrue();

    Set<String> firstLevelKeyset = new HashSet<>(results.right);
    assertThat(firstLevelKeyset.size()).isEqualTo(3);
    assertThat(firstLevelKeyset).contains("some", "array", "nested");
  }

  @Test
  public void shredJson_happyPathEmptyObject() {
    List<Object[]> expectedRows = getExpectedEmptyObjectRows();
    String payload = "{\"some\":{}, \"array\":[[], true, 3], \"nested\": {\"structure\": 23.456}}";
    ImmutablePair<List<Object[]>, List<String>> results =
        service.shredJson(surfer, Collections.emptyList(), "documentId", payload, false, false);
    List<Object[]> bindParams = results.left;
    assertThat(bindParams.size()).isEqualTo(expectedRows.size());
    assertThat(arraysMatch(bindParams.get(0), expectedRows.get(0))).isTrue();
    assertThat(arraysMatch(bindParams.get(1), expectedRows.get(1))).isTrue();
    assertThat(arraysMatch(bindParams.get(2), expectedRows.get(2))).isTrue();
    assertThat(arraysMatch(bindParams.get(3), expectedRows.get(3))).isTrue();
    assertThat(arraysMatch(bindParams.get(4), expectedRows.get(4))).isTrue();

    Set<String> firstLevelKeyset = new HashSet<>(results.right);
    assertThat(firstLevelKeyset.size()).isEqualTo(3);
    assertThat(firstLevelKeyset).contains("some", "array", "nested");
  }

  @Test
  public void shredJson_emptyObject() {
    String payload = "{}";
    ImmutablePair<List<Object[]>, List<String>> results =
        service.shredJson(surfer, Collections.emptyList(), "documentId", payload, false, false);
    List<Object[]> bindParams = results.left;
    assertThat(bindParams.size()).isEqualTo(0);

    Set<String> firstLevelKeyset = new HashSet<>(results.right);
    assertThat(firstLevelKeyset.size()).isEqualTo(0);
  }

  @Test
  public void shredJson_errorPatching() {
    String payload = "[1, true, 3]";
    assertThatThrownBy(
            () ->
                service.shredJson(
                    surfer, Collections.emptyList(), "documentId", payload, true, false))
        .hasMessage("A patch operation must be done with a JSON object, not an array.");
  }

  @Test
  public void shredJson_errorMaxDepth() {
    String payload =
        "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"tooDeep\"]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
    assertThatThrownBy(
            () ->
                service.shredJson(
                    surfer, Collections.emptyList(), "documentId", payload, false, false))
        .hasMessage("Max depth of 64 exceeded.");
  }

  private Object[] emptyStringArray(int size) {
    Object[] array = new Object[size];
    for (int i = 0; i < array.length; i++) {
      array[i] = "";
    }
    return array;
  }

  private List<Object[]> getExpectedRows() {
    Object[] expectedRow0 = emptyStringArray(69);
    expectedRow0[0] = "documentId";
    expectedRow0[1] = "some";
    expectedRow0[65] = "some";
    expectedRow0[66] = "value";
    expectedRow0[67] = null;
    expectedRow0[68] = null;

    Object[] expectedRow1 = emptyStringArray(69);
    expectedRow1[0] = "documentId";
    expectedRow1[1] = "array";
    expectedRow1[2] = "[000000]";
    expectedRow1[65] = "[000000]";
    expectedRow1[66] = null;
    expectedRow1[67] = 1.0;
    expectedRow1[68] = null;

    Object[] expectedRow2 = emptyStringArray(69);
    expectedRow2[0] = "documentId";
    expectedRow2[1] = "array";
    expectedRow2[2] = "[000001]";
    expectedRow2[65] = "[000001]";
    expectedRow2[66] = null;
    expectedRow2[67] = null;
    expectedRow2[68] = true;

    Object[] expectedRow3 = emptyStringArray(69);
    expectedRow3[0] = "documentId";
    expectedRow3[1] = "array";
    expectedRow3[2] = "[000002]";
    expectedRow3[65] = "[000002]";
    expectedRow3[66] = null;
    expectedRow3[67] = 3.0;
    expectedRow3[68] = null;

    Object[] expectedRow4 = emptyStringArray(69);
    expectedRow4[0] = "documentId";
    expectedRow4[1] = "nested";
    expectedRow4[2] = "structure";
    expectedRow4[65] = "structure";
    expectedRow4[66] = null;
    expectedRow4[67] = 23.456;
    expectedRow4[68] = null;

    return ImmutableList.of(expectedRow0, expectedRow1, expectedRow2, expectedRow3, expectedRow4);
  }

  private List<Object[]> getExpectedEmptyObjectRows() {
    Object[] expectedRow0 = emptyStringArray(69);
    expectedRow0[0] = "documentId";
    expectedRow0[1] = "some";
    expectedRow0[65] = "some";
    expectedRow0[66] = DocsApiConstants.EMPTY_OBJECT_MARKER;
    expectedRow0[67] = null;
    expectedRow0[68] = null;

    Object[] expectedRow1 = emptyStringArray(69);
    expectedRow1[0] = "documentId";
    expectedRow1[1] = "array";
    expectedRow1[2] = "[000000]";
    expectedRow1[65] = "[000000]";
    expectedRow1[66] = DocsApiConstants.EMPTY_ARRAY_MARKER;
    expectedRow1[67] = null;
    expectedRow1[68] = null;

    Object[] expectedRow2 = emptyStringArray(69);
    expectedRow2[0] = "documentId";
    expectedRow2[1] = "array";
    expectedRow2[2] = "[000001]";
    expectedRow2[65] = "[000001]";
    expectedRow2[66] = null;
    expectedRow2[67] = null;
    expectedRow2[68] = true;

    Object[] expectedRow3 = emptyStringArray(69);
    expectedRow3[0] = "documentId";
    expectedRow3[1] = "array";
    expectedRow3[2] = "[000002]";
    expectedRow3[65] = "[000002]";
    expectedRow3[66] = null;
    expectedRow3[67] = 3.0;
    expectedRow3[68] = null;

    Object[] expectedRow4 = emptyStringArray(69);
    expectedRow4[0] = "documentId";
    expectedRow4[1] = "nested";
    expectedRow4[2] = "structure";
    expectedRow4[65] = "structure";
    expectedRow4[66] = null;
    expectedRow4[67] = 23.456;
    expectedRow4[68] = null;

    return ImmutableList.of(expectedRow0, expectedRow1, expectedRow2, expectedRow3, expectedRow4);
  }

  private List<Object[]> getNestedExpectedRows() {
    Object[] expectedRow0 = emptyStringArray(69);
    expectedRow0[0] = "documentId";
    expectedRow0[1] = "nested";
    expectedRow0[2] = "path";
    expectedRow0[3] = "spot";
    expectedRow0[4] = "some";
    expectedRow0[65] = "some";
    expectedRow0[66] = "value";
    expectedRow0[67] = null;
    expectedRow0[68] = null;

    Object[] expectedRow1 = emptyStringArray(69);
    expectedRow1[0] = "documentId";
    expectedRow1[1] = "nested";
    expectedRow1[2] = "path";
    expectedRow1[3] = "spot";
    expectedRow1[4] = "array";
    expectedRow1[5] = "[000000]";
    expectedRow1[65] = "[000000]";
    expectedRow1[66] = null;
    expectedRow1[67] = 1.0;
    expectedRow1[68] = null;

    Object[] expectedRow2 = emptyStringArray(69);
    expectedRow2[0] = "documentId";
    expectedRow2[1] = "nested";
    expectedRow2[2] = "path";
    expectedRow2[3] = "spot";
    expectedRow2[4] = "array";
    expectedRow2[5] = "[000001]";
    expectedRow2[65] = "[000001]";
    expectedRow2[66] = null;
    expectedRow2[67] = null;
    expectedRow2[68] = true;

    Object[] expectedRow3 = emptyStringArray(69);
    expectedRow3[0] = "documentId";
    expectedRow3[1] = "nested";
    expectedRow3[2] = "path";
    expectedRow3[3] = "spot";
    expectedRow3[4] = "array";
    expectedRow3[5] = "[000002]";
    expectedRow3[65] = "[000002]";
    expectedRow3[66] = null;
    expectedRow3[67] = 3.0;
    expectedRow3[68] = null;

    Object[] expectedRow4 = emptyStringArray(69);
    expectedRow4[0] = "documentId";
    expectedRow4[1] = "nested";
    expectedRow4[2] = "path";
    expectedRow4[3] = "spot";
    expectedRow4[4] = "nested";
    expectedRow4[5] = "structure";
    expectedRow4[65] = "structure";
    expectedRow4[66] = null;
    expectedRow4[67] = 23.456;
    expectedRow4[68] = null;

    return ImmutableList.of(expectedRow0, expectedRow1, expectedRow2, expectedRow3, expectedRow4);
  }
}
