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
package io.stargate.web.docsapi.service;

import static io.stargate.web.docsapi.service.CombinedPagingState.deserialize;
import static io.stargate.web.docsapi.service.CombinedPagingState.serialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.core.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class CombinedPagingStateTest {

  @Test
  void testSingleNestedRoundTrip() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
    List<ByteBuffer> combined = deserialize(1, serialize(ImmutableList.of(state)));
    assertThat(combined).hasSize(1);
    assertThat(combined.get(0).array()).isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcut() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
    assertThat(serialize(ImmutableList.of(state)))
        .isNotNull()
        .extracting(ByteBuffer::array)
        .isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcutEmpty() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {});
    assertThat(serialize(ImmutableList.of(state)))
        .isNotNull()
        .extracting(ByteBuffer::array)
        .isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcutNull() {
    assertThat(serialize(Collections.singletonList(null))).isNull();
  }

  @Test
  void testMultipleNestedRoundTrip() {
    ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
    ByteBuffer s1 = ByteBuffer.wrap(new byte[0]);
    ByteBuffer s2 = ByteBuffer.wrap(new byte[] {-1, -3});
    List<ByteBuffer> combined = deserialize(3, serialize(ImmutableList.of(s0, s1, s2)));
    assertThat(combined).hasSize(3);
    assertThat(ByteBufferUtils.getArray(combined.get(0))).isEqualTo(s0.array());
    assertThat(ByteBufferUtils.getArray(combined.get(1))).isEqualTo(s1.array());
    assertThat(ByteBufferUtils.getArray(combined.get(2))).isEqualTo(s2.array());
  }

  @Test
  void testMultipleNestedRoundTripWithNull() {
    ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
    ByteBuffer s1 = ByteBuffer.wrap(new byte[] {-1, -3});

    List<ByteBuffer> combined = deserialize(3, serialize(Arrays.asList(s0, null, s1)));
    assertThat(combined).hasSize(3);
    assertThat(ByteBufferUtils.getArray(combined.get(0))).isEqualTo(s0.array());
    assertThat(combined.get(1)).isNull();
    assertThat(ByteBufferUtils.getArray(combined.get(2))).isEqualTo(s1.array());

    combined = deserialize(3, serialize(Arrays.asList(null, s0, null)));
    assertThat(combined).hasSize(3);
    assertThat(combined.get(0)).isNull();
    assertThat(ByteBufferUtils.getArray(combined.get(1))).isEqualTo(s0.array());
    assertThat(combined.get(2)).isNull();
  }

  @Test
  void testMultipleNestedRoundTripAllNull() {
    assertThat(serialize(Arrays.asList(null, null))).isNull();
  }

  @Test
  void testNullState() {
    List<ByteBuffer> combined = deserialize(5, null);
    assertThat(combined).hasSize(5);
    assertThat(combined).allMatch(Objects::isNull);
  }

  @Test
  void testExpectedSize() {
    ByteBuffer s = ByteBuffer.wrap(new byte[] {1, 2});

    assertThatThrownBy(() -> deserialize(-111, serialize(ImmutableList.of(s, s))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("-111"); // invalid expected size

    assertThatThrownBy(() -> deserialize(245, ByteBuffer.wrap(new byte[3])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("available bytes: 3");

    assertThatThrownBy(() -> deserialize(1345, serialize(ImmutableList.of(s, s))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2") // actual number of sub-states
        .hasMessageContaining("1345"); // expected number of sub-states
  }
}
