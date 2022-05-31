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

package io.stargate.sgv2.docsapi.service.query.model.paging;

import static io.stargate.sgv2.docsapi.service.query.model.paging.CombinedPagingState.EXHAUSTED_PAGE_STATE;
import static io.stargate.sgv2.docsapi.service.query.model.paging.CombinedPagingState.deserialize;
import static io.stargate.sgv2.docsapi.service.query.model.paging.CombinedPagingState.serialize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CombinedPagingStateTest {

  @Nested
  class IsExhausted {

    @Test
    void constant() {
      assertThat(CombinedPagingState.isExhausted(EXHAUSTED_PAGE_STATE)).isTrue();
    }

    @Test
    void custom() {
      assertThat(CombinedPagingState.isExhausted(ByteBuffer.allocate(0))).isTrue();
      assertThat(CombinedPagingState.isExhausted(ByteBuffer.allocate(1))).isFalse();
    }

    @Test
    void nullState() {
      assertThat(CombinedPagingState.isExhausted(null)).isFalse();
    }
  }

  @Nested
  class MakePagingState {

    @Test
    void oneNull() {
      CombinedPagingState combined = new CombinedPagingState(ImmutableList.of(resumeMode -> null));
      assertThat(combined.makePagingState(ResumeMode.NEXT_PARTITION)).isNull();
    }

    @Test
    void allNull() {
      CombinedPagingState combined =
          new CombinedPagingState(ImmutableList.of(resumeMode -> null, resumeMode -> null));
      assertThat(combined.makePagingState(ResumeMode.NEXT_PARTITION)).isNull();
    }

    @Test
    void mixed() {
      CombinedPagingState combined =
          new CombinedPagingState(
              ImmutableList.of(resumeMode -> null, resumeMode -> EXHAUSTED_PAGE_STATE));
      assertThat(combined.makePagingState(ResumeMode.NEXT_PARTITION)).isNotNull();
    }

    @Test
    void exhausted() {
      CombinedPagingState combined =
          new CombinedPagingState(ImmutableList.of(resumeMode -> EXHAUSTED_PAGE_STATE));
      assertThat(combined.makePagingState(ResumeMode.NEXT_PARTITION)).isNull();
    }
  }

  @Nested
  class Serialization {

    @Test
    void singleNestedRoundTrip() {
      ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
      List<ByteBuffer> combined = deserialize(1, serialize(ImmutableList.of(state)));
      assertThat(combined).hasSize(1);
      assertThat(combined.get(0).array()).isEqualTo(state.array());
    }

    @Test
    void singleNestedShortcut() {
      ByteBuffer state = ByteBuffer.wrap(new byte[] {1, 2});
      assertThat(serialize(ImmutableList.of(state)))
          .isNotNull()
          .extracting(ByteBuffer::array)
          .isEqualTo(state.array());
    }

    @Test
    void singleNestedShortcutEmpty() {
      ByteBuffer state = ByteBuffer.wrap(new byte[] {});
      assertThat(serialize(ImmutableList.of(state)))
          .isNotNull()
          .extracting(ByteBuffer::array)
          .isEqualTo(state.array());
    }

    @Test
    void singleNestedShortcutNull() {
      assertThat(serialize(Collections.singletonList(null))).isNull();
    }

    @Test
    void multipleNestedRoundTrip() {
      ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
      ByteBuffer s1 = ByteBuffer.wrap(new byte[0]);
      ByteBuffer s2 = ByteBuffer.wrap(new byte[] {-1, -3});
      List<ByteBuffer> combined = deserialize(3, serialize(ImmutableList.of(s0, s1, s2)));
      assertThat(combined).hasSize(3);
      assertThat(getArray(combined.get(0))).isEqualTo(s0.array());
      assertThat(getArray(combined.get(1))).isEqualTo(s1.array());
      assertThat(getArray(combined.get(2))).isEqualTo(s2.array());
    }

    @Test
    void multipleNestedRoundTripWithNull() {
      ByteBuffer s0 = ByteBuffer.wrap(new byte[] {1, 2});
      ByteBuffer s1 = ByteBuffer.wrap(new byte[] {-1, -3});

      List<ByteBuffer> combined = deserialize(3, serialize(Arrays.asList(s0, null, s1)));
      assertThat(combined).hasSize(3);
      assertThat(getArray(combined.get(0))).isEqualTo(s0.array());
      assertThat(combined.get(1)).isNull();
      assertThat(getArray(combined.get(2))).isEqualTo(s1.array());

      combined = deserialize(3, serialize(Arrays.asList(null, s0, null)));
      assertThat(combined).hasSize(3);
      assertThat(combined.get(0)).isNull();
      assertThat(getArray(combined.get(1))).isEqualTo(s0.array());
      assertThat(combined.get(2)).isNull();
    }

    @Test
    void multipleNestedRoundTripAllNull() {
      assertThat(serialize(Arrays.asList(null, null))).isNull();
    }

    @Test
    void nullState() {
      List<ByteBuffer> combined = deserialize(5, null);
      assertThat(combined).hasSize(5);
      assertThat(combined).allMatch(Objects::isNull);
    }

    @Test
    void expectedSize() {
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

    /**
     * Extract the content of the provided {@code ByteBuffer} as a byte array.
     *
     * <p>This method work with any type of {@code ByteBuffer} (direct and non-direct ones), but
     * when the {@code ByteBuffer} is backed by an array, this method will try to avoid copy when
     * possible. As a consequence, changes to the returned byte array may or may not reflect into
     * the initial {@code ByteBuffer}.
     *
     * @param bytes the buffer whose content to extract.
     * @return a byte array with the content of {@code bytes}. That array may be the array backing
     *     {@code bytes} if this can avoid a copy.
     */
    public static byte[] getArray(ByteBuffer bytes) {
      int length = bytes.remaining();

      if (bytes.hasArray()) {
        int boff = bytes.arrayOffset() + bytes.position();
        if (boff == 0 && length == bytes.array().length) return bytes.array();
        else return Arrays.copyOfRange(bytes.array(), boff, boff + length);
      }
      // else, DirectByteBuffer.get() is the fastest route
      byte[] array = new byte[length];
      bytes.duplicate().get(array);
      return array;
    }
  }
}
