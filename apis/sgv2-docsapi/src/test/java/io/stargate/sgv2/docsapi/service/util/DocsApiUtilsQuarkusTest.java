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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class DocsApiUtilsQuarkusTest {

  @Inject DocsApiTestSchemaProvider schemaProvider;
  @Inject DocumentProperties documentProperties;

  @Nested
  class IsRowMatchingPath {

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().leafColumnName(),
                  Values.of("value"),
                  documentProperties.tableProperties().pathColumnName(0),
                  Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1),
                  Values.of("value"),
                  documentProperties.tableProperties().pathColumnName(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().leafColumnName(),
                  Values.of("commas."),
                  documentProperties.tableProperties().pathColumnName(0),
                  Values.of("field,with"),
                  documentProperties.tableProperties().pathColumnName(1),
                  Values.of("commas."),
                  documentProperties.tableProperties().pathColumnName(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void notMatchingExtraDepth() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().leafColumnName(),
                  Values.of("value"),
                  documentProperties.tableProperties().pathColumnName(0),
                  Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1),
                  Values.of("value"),
                  documentProperties.tableProperties().pathColumnName(2),
                  Values.of("value"),
                  documentProperties.tableProperties().pathColumnName(3),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }

    @Test
    public void notMatchingWrongField() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().leafColumnName(),
                  Values.of("other"),
                  documentProperties.tableProperties().pathColumnName(0),
                  Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1),
                  Values.of("other"),
                  documentProperties.tableProperties().pathColumnName(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class IsRowOnPath {

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("value")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field,with"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("commas.")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSubPath() {
      List<String> path = Collections.singletonList("field");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("more")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("value2")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingGlob() {
      List<String> path = Arrays.asList("*", "[*]");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("[000001]")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void pathNotMatchingDifferent() {
      List<String> path = Arrays.asList("parent", "field");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("parent")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }

    @Test
    public void pathNotMatchingTooShort() {
      List<String> path = Arrays.asList("parent", "field");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("parent"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }

    @Test
    public void notMatchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("noValue")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }

    @Test
    public void matchingMatchingGlobArray() {
      List<String> path = Arrays.asList("field", "*");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0), Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1), Values.of("[000001]")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingMatchingGlob() {
      List<String> path = Arrays.asList("field", "[*]");
      RowWrapper row =
          schemaProvider.getRow(
              ImmutableMap.of(
                  documentProperties.tableProperties().pathColumnName(0),
                  Values.of("field"),
                  documentProperties.tableProperties().pathColumnName(1),
                  Values.of("value")));

      boolean result = DocsApiUtils.isRowOnPath(row, path, documentProperties);

      assertThat(result).isFalse();
    }
  }
}
