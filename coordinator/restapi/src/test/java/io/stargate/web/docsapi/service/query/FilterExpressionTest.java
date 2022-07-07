/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.web.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.db.datastore.MapBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FilterExpressionTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
  private static final Table TABLE = SCHEMA_PROVIDER.getTable();

  @Mock FilterPath filterPath;

  @Mock BaseCondition condition;

  @Mock BaseCondition condition2;

  @Nested
  class CollectK {

    @Test
    public void happyPath() {
      Expression<FilterExpression> expression =
          And.of(
              ImmutableFilterExpression.of(filterPath, condition, 0),
              ImmutableFilterExpression.of(filterPath, condition2, 1));

      Set<FilterExpression> filterExpressions = new HashSet<>();
      expression.collectK(filterExpressions, Integer.MAX_VALUE);

      assertThat(filterExpressions).hasSize(2);
    }
  }

  @Nested
  class TestRawDocument {

    @Mock RawDocument document;

    @Test
    public void singleRowMatch() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(document.rows()).thenReturn(Collections.singletonList(row));
      when(condition.test(row)).thenReturn(true);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(document);

      assertThat(result).isTrue();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void multipleRowsOneMatch() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row1 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      Row row2 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(document.rows()).thenReturn(Arrays.asList(row1, row2));
      when(condition.test(row2)).thenReturn(true);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(document);

      assertThat(result).isTrue();
      verify(condition).test(row2);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void singleRowConditionDoesMatch() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(document.rows()).thenReturn(Collections.singletonList(row));
      when(condition.test(row)).thenReturn(false);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(document);

      assertThat(result).isFalse();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void multipleRowsNoOneMatch() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row1 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      Row row2 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(document.rows()).thenReturn(Arrays.asList(row1, row2));
      when(condition.test(row2)).thenReturn(false);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(document);

      assertThat(result).isFalse();
      verify(condition).test(row2);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void noRowOnFilterPath() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row1 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      Row row2 =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "extra",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "extra",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(document.rows()).thenReturn(Arrays.asList(row1, row2));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(document);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }
  }

  @Nested
  class TestRow {

    @Test
    public void conditionTrue() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(condition.test(row)).thenReturn(true);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void conditionTrueGlob() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("*", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(condition.test(row)).thenReturn(true);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void conditionTrueArrayGlob() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("[*]", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "[000001]",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(condition.test(row)).thenReturn(true);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void conditionFalse() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(condition.test(row)).thenReturn(false);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isFalse();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void conditionFalsePathSegment() {
      ImmutableFilterPath filterPath =
          ImmutableFilterPath.of(Arrays.asList("parent,other", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));
      when(condition.test(row)).thenReturn(false);

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isFalse();
      verify(condition).test(row);
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void pathNotMatchingLeafDoesNotMatchField() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(TABLE, ImmutableMap.of(DocsApiConstants.LEAF_COLUMN_NAME, "whatever"));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void pathNotMatchingLongerPaths() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "more"));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void pathNotMatchingDifferent() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }
  }

  @Nested
  class Negate {

    @Test
    void selectivity() {
      FilterExpression filter = ImmutableFilterExpression.of(filterPath, condition, 0, 0.2);

      when(condition.negate()).thenReturn(condition2);

      assertThat(filter.negate().getSelectivity()).isEqualTo(0.8);
    }

    @Test
    void condition() {
      FilterExpression filter = ImmutableFilterExpression.of(filterPath, condition, 0, 0.2);

      when(condition.negate()).thenReturn(condition2);

      assertThat(filter.negate().getCondition()).isEqualTo(condition2);
    }
  }
}
