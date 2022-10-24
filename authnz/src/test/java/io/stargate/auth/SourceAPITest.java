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

package io.stargate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SourceAPITest {

  @Nested
  class ToCustomPayload {

    @Test
    public void happyPath() {
      Map<String, ByteBuffer> map = new HashMap<>();

      SourceAPI.GRAPHQL.toCustomPayload(map);

      assertThat(map).containsKey("stargate.sourceAPI");
    }

    @Test
    public void nullProtected() {
      SourceAPI.GRAPHQL.toCustomPayload(null);
    }
  }

  @Nested
  class FromCustomPayload {

    @Test
    public void happyPath() {
      Map<String, ByteBuffer> map = new HashMap<>();
      SourceAPI.GRAPHQL.toCustomPayload(map);

      SourceAPI result = SourceAPI.fromCustomPayload(map, SourceAPI.CQL);

      assertThat(result).isEqualTo(SourceAPI.GRAPHQL);
    }

    @Test
    public void emptyMap() {
      Map<String, ByteBuffer> map = new HashMap<>();

      SourceAPI result = SourceAPI.fromCustomPayload(map, SourceAPI.CQL);

      assertThat(result).isEqualTo(SourceAPI.CQL);
    }

    @Test
    public void nullMap() {
      Map<String, ByteBuffer> map = new HashMap<>();

      SourceAPI result = SourceAPI.fromCustomPayload(map, SourceAPI.REST);

      assertThat(result).isEqualTo(SourceAPI.REST);
    }
  }
}
