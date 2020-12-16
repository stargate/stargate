package io.stargate.api.sql.server.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class SetStatementTest {

  @Test
  public void select() {
    assertThat(SetStatement.parse("SELECT x from y")).isNull();
  }

  @Test
  public void usingEqualsMixedCase() {
    SetStatement parsed = SetStatement.parse("SeT abc = 123");
    assertThat(parsed).isNotNull();
    assertThat(parsed.getKey()).isEqualTo("abc");
    assertThat(parsed.getValue()).isEqualTo("123");
  }

  @Test
  public void usingTo() {
    SetStatement parsed = SetStatement.parse("SET abc TO 'xyz'");
    assertThat(parsed).isNotNull();
    assertThat(parsed.getKey()).isEqualTo("abc");
    assertThat(parsed.getValue()).isEqualTo("'xyz'");
  }
}
