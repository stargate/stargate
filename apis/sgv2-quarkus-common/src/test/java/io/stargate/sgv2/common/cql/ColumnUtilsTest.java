/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.common.cql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ColumnUtilsTest {

  @Test
  @DisplayName("Should quote CQL identifier if needed")
  public void maybeQuote() {
    assertThat(ColumnUtils.maybeQuote("foo")).isEqualTo("foo");
    // Mixed case:
    assertThat(ColumnUtils.maybeQuote("Foo")).isEqualTo("\"Foo\"");
    // Special characters:
    assertThat(ColumnUtils.maybeQuote("foo bar")).isEqualTo("\"foo bar\"");
    // Reserved keywords:
    assertThat(ColumnUtils.maybeQuote("table")).isEqualTo("\"table\"");
    // If we quote, existing quotes must be escaped:
    assertThat(ColumnUtils.maybeQuote("a\"a")).isEqualTo("\"a\"\"a\"");
  }
}
