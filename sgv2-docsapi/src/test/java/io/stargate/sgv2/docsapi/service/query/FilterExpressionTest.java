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

package io.stargate.sgv2.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.google.common.collect.ImmutableMap;
import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.RawDocument;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row1 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
      RowWrapper row2 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row1 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
      RowWrapper row2 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row1 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
      RowWrapper row2 =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("extra"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("extra"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("[000001]"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));
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
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(DocsApiConstants.LEAF_COLUMN_NAME, Values.of("whatever")));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void pathNotMatchingLongerPaths() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("more")));

      FilterExpression expression = ImmutableFilterExpression.of(filterPath, condition, 0);
      boolean result = expression.test(row);

      assertThat(result).isTrue();
      verifyNoMoreInteractions(condition);
    }

    @Test
    public void pathNotMatchingDifferent() {
      ImmutableFilterPath filterPath = ImmutableFilterPath.of(Arrays.asList("parent", "field"));
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));

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
