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

import org.junit.jupiter.api.Test;

public class CqlStringsTest {
  @Test
  public void shouldDoubleQuoteUdts() {
    assertThat(CqlStrings.doubleQuoteUdts("int")).isEqualTo("int");
    assertThat(CqlStrings.doubleQuoteUdts("text")).isEqualTo("text");
    assertThat(CqlStrings.doubleQuoteUdts("'CustomType'")).isEqualTo("'CustomType'");
    assertThat(CqlStrings.doubleQuoteUdts("list<map<int, text>>")).isEqualTo("list<map<int,text>>");
    assertThat(CqlStrings.doubleQuoteUdts("frozen<set<bigint>>")).isEqualTo("frozen<set<bigint>>");

    assertThat(CqlStrings.doubleQuoteUdts("address")).isEqualTo("\"address\"");
    assertThat(CqlStrings.doubleQuoteUdts("Address")).isEqualTo("\"Address\"");
    assertThat(CqlStrings.doubleQuoteUdts("frozen<list<map<text,address>>>"))
        .isEqualTo("frozen<list<map<text,\"address\">>>");

    // Corner-case: these should always be parameterized in regular CQL, so if they occur on their
    // own we'd have to assume it's a custom UDT.
    assertThat(CqlStrings.doubleQuoteUdts("list")).isEqualTo("\"list\"");
    assertThat(CqlStrings.doubleQuoteUdts("frozen")).isEqualTo("\"frozen\"");
  }
}
