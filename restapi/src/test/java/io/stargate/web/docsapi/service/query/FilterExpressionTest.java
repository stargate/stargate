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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FilterExpressionTest {

  @Mock FilterPath filterPath;

  @Mock BaseCondition condition1;

  @Mock BaseCondition condition2;

  @Nested
  class CollectK {

    @Test
    public void happyPath() {
      Expression<FilterExpression> expression =
          And.of(
              ImmutableFilterExpression.of(filterPath, condition1),
              ImmutableFilterExpression.of(filterPath, condition2));

      Set<FilterExpression> filterExpressions = new HashSet<>();
      expression.collectK(filterExpressions, Integer.MAX_VALUE);

      assertThat(filterExpressions).hasSize(2);
    }
  }
}
