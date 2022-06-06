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

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A utility class for combining one or more per-query paging state buffers into one byte buffer.
 *
 * <p>It handles the edge cases of {@code null} and {@link #EXHAUSTED_PAGE_STATE "exhausted"} query
 * states.
 */
public class CombinedPagingState implements PagingStateSupplier {

  /**
   * A special paging state buffer that indicates that the query reach the end of its result set.
   *
   * <p>Normally individual queries return a {@code null} paging state when they are exhausted.
   * However, if case of merging two or more results sets we need to distinguish queries that have
   * not yet produced any rows (also having a {@code null} initial paging state) from queries that
   * reach the end of their result sets.
   */
  public static final ByteBuffer EXHAUSTED_PAGE_STATE = ByteBuffer.allocate(0);

  private final List<PagingStateSupplier> pagingState;

  public CombinedPagingState(List<PagingStateSupplier> pagingState) {
    this.pagingState = pagingState;
  }

  public static boolean isExhausted(ByteBuffer pagingState) {
    return pagingState != null && pagingState.remaining() == 0;
  }

  @Override
  public ByteBuffer makePagingState() {
    List<ByteBuffer> pagingStateBuffers =
        pagingState.stream().map(PagingStateSupplier::makePagingState).collect(Collectors.toList());

    // An empty paging state means the query was exhausted during previous execution.
    // A null paging state mean the query has not been executed yet.
    if (pagingStateBuffers.stream().allMatch(CombinedPagingState::isExhausted)) {
      // Return a null overall paging state if all queries have been exhausted.
      return null;
    }

    return CombinedPagingState.serialize(pagingStateBuffers);
  }

  public static ByteBuffer serialize(List<ByteBuffer> nestedStates) {
    if (nestedStates.stream().allMatch(Objects::isNull)) {
      return null;
    }

    if (nestedStates.size() == 1) {
      return nestedStates.get(0);
    }

    int toAllocate = Integer.BYTES; // int for element count
    for (ByteBuffer state : nestedStates) {
      toAllocate += Integer.BYTES; // int for element size
      toAllocate += state == null ? 0 : state.remaining();
    }

    ByteBuffer result = ByteBuffer.allocate(toAllocate);

    result.putInt(nestedStates.size());
    for (ByteBuffer state : nestedStates) {
      if (state != null) {
        ByteBuffer nested = state.slice();

        result.putInt(nested.remaining());
        result.put(nested);
      } else {
        result.putInt(-1); // no state
      }
    }

    result.flip();
    return result;
  }

  /**
   * Breaks down a combined paging state into per-query paging state buffers.
   *
   * @param expectedSize the expected number of nested paging states. This number servers as
   *     simplistic sanity check to ensure we have as many queries using the paging states as we
   *     have nested elements. Also it is used to support rolling upgrades where we have to be able
   *     to correctly interpret one-element paging states in the old format.
   * @param data serialized form of the combined paging state
   */
  public static List<ByteBuffer> deserialize(int expectedSize, ByteBuffer data) {
    if (expectedSize <= 0) {
      throw new IllegalArgumentException("Invalid paging state size: " + expectedSize);
    }

    if (data == null) {
      ArrayList<ByteBuffer> buffers = new ArrayList<>(expectedSize);
      for (int i = 0; i < expectedSize; i++) {
        buffers.add(null);
      }

      return Collections.unmodifiableList(buffers);
    }

    // The special case for one nested element is mostly to support rolling upgrades where older
    // nodes may be producing one-elelement paging states, while newer (upgraded) nodes might have
    // to interpret those paging states.
    if (expectedSize == 1) {
      return ImmutableList.of(data);
    }

    if (data.remaining() < Integer.BYTES) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid paging state: unable to read size, available bytes: %d", data.remaining()));
    }

    int count = data.getInt();
    if (expectedSize != count) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid paging state: expected element count: %d, actual: %d", expectedSize, count));
    }

    ArrayList<ByteBuffer> nested = new ArrayList<>(count);
    while (count-- > 0) {
      int size = data.getInt();

      ByteBuffer element;
      if (size >= 0) {
        element = data.slice();
        element.limit(size);
        data.position(data.position() + size);
      } else {
        element = null;
      }

      nested.add(element);
    }

    return Collections.unmodifiableList(nested);
  }
}
