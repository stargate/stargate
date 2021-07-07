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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.core.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class CombinedPagingStateTest {

  @Test
  void testSingleNestedRoundTrip() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
    CombinedPagingState c0 = CombinedPagingState.of(ImmutableList.of(state));
    CombinedPagingState c1 = CombinedPagingState.deserialize(1, c0.serialize());
    assertThat(c1.nested()).hasSize(1);
    assertThat(c1.nested().get(0).array()).isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcut() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
    CombinedPagingState c0 = CombinedPagingState.of(ImmutableList.of(state));
    assertThat(c0.serialize().array()).isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcutEmpty() {
    ByteBuffer state = ByteBuffer.wrap(new byte[] {});
    CombinedPagingState c0 = CombinedPagingState.of(ImmutableList.of(state));
    assertThat(c0.serialize().array()).isEqualTo(state.array());
  }

  @Test
  void testSingleNestedShortcutNull() {
    CombinedPagingState c0 = CombinedPagingState.of(Collections.singletonList(null));
    assertThat(c0.serialize()).isNull();
  }

  @Test
  void testMultipleNestedRoundTrip() {
    ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
    ByteBuffer s1 = ByteBuffer.wrap(new byte[0]);
    ByteBuffer s2 = ByteBuffer.wrap(new byte[] {-1, -3});
    CombinedPagingState orig = CombinedPagingState.of(ImmutableList.of(s0, s1, s2));
    CombinedPagingState state = CombinedPagingState.deserialize(3, orig.serialize());
    assertThat(state.nested()).hasSize(3);
    assertThat(ByteBufferUtils.getArray(state.nested().get(0))).isEqualTo(s0.array());
    assertThat(ByteBufferUtils.getArray(state.nested().get(1))).isEqualTo(s1.array());
    assertThat(ByteBufferUtils.getArray(state.nested().get(2))).isEqualTo(s2.array());
  }

  @Test
  void testMultipleNestedRoundTripWithNull() {
    ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
    ByteBuffer s1 = ByteBuffer.wrap(new byte[] {-1, -3});

    CombinedPagingState orig = CombinedPagingState.of(Arrays.asList(s0, null, s1));
    CombinedPagingState state = CombinedPagingState.deserialize(3, orig.serialize());
    assertThat(state.nested()).hasSize(3);
    assertThat(ByteBufferUtils.getArray(state.nested().get(0))).isEqualTo(s0.array());
    assertThat(state.nested().get(1)).isNull();
    assertThat(ByteBufferUtils.getArray(state.nested().get(2))).isEqualTo(s1.array());

    orig = CombinedPagingState.of(Arrays.asList(null, s0, null));
    state = CombinedPagingState.deserialize(3, orig.serialize());
    assertThat(state.nested()).hasSize(3);
    assertThat(state.nested().get(0)).isNull();
    assertThat(ByteBufferUtils.getArray(state.nested().get(1))).isEqualTo(s0.array());
    assertThat(state.nested().get(2)).isNull();
  }

  @Test
  void testMultipleNestedRoundTripAllNull() {
    CombinedPagingState orig = CombinedPagingState.of(Arrays.asList(null, null));
    assertThat(orig.serialize()).isNull();
  }

  @Test
  void testNullState() {
    CombinedPagingState c = CombinedPagingState.deserialize(5, null);
    assertThat(c.nested()).hasSize(5);
    assertThat(c.nested()).allMatch(Objects::isNull);
  }

  @Test
  void testExpectedSize() {
    ByteBuffer s = ByteBuffer.wrap(new byte[] {1, 2});
    CombinedPagingState c1 = CombinedPagingState.of(ImmutableList.of(s, s));

    assertThatThrownBy(() -> CombinedPagingState.deserialize(-111, c1.serialize()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("-111");

    assertThatThrownBy(() -> CombinedPagingState.deserialize(245, ByteBuffer.wrap(new byte[3])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("available bytes: 3");

    assertThatThrownBy(() -> CombinedPagingState.deserialize(1345, c1.serialize()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2") // actual number of sub-states
        .hasMessageContaining("1345"); // expected number of sub-states
  }
}
