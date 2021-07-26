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

package io.stargate.web.docsapi.service.query.eval;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.eval.EvalEngine;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EvalFilterExpressionTest {

  @Mock FilterExpression expression;

  @Mock Row row;

  @Nested
  class Evaluate {

    @Test
    public void evalTrue() {
      List<Row> rows = Collections.singletonList(row);
      when(expression.test(rows)).thenReturn(true);
      when(expression.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);

      EvalFilterExpression eval = new EvalFilterExpression(rows);
      boolean result =
          EvalEngine.evaluate(
              expression, Collections.singletonMap(FilterExpression.EXPR_TYPE, eval));

      assertThat(result).isTrue();
      verify(expression).test(rows);
      verify(expression).getExprType();
      verifyNoMoreInteractions(expression);
    }

    @Test
    public void evalFalse() {
      List<Row> rows = Collections.singletonList(row);
      when(expression.test(rows)).thenReturn(false);
      when(expression.getExprType()).thenReturn(FilterExpression.EXPR_TYPE);

      EvalFilterExpression eval = new EvalFilterExpression(rows);
      boolean result =
          EvalEngine.evaluate(
              expression, Collections.singletonMap(FilterExpression.EXPR_TYPE, eval));

      assertThat(result).isFalse();
      verify(expression).test(rows);
      verify(expression).getExprType();
      verifyNoMoreInteractions(expression);
    }
  }
}
