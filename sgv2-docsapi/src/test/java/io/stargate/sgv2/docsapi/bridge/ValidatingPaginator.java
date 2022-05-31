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
package io.stargate.sgv2.docsapi.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ValidatingPaginator {

  public static final int MAGIC = 1020304050;
  private final int pageSize;
  private final int offset;
  private int nextOffset = -1;

  private ValidatingPaginator(int pageSize, int offset) {
    this.pageSize = pageSize;
    this.offset = offset;
  }

  public static ValidatingPaginator of(int pageSize) {
    return new ValidatingPaginator(pageSize, -1);
  }

  public static ValidatingPaginator of(int pageSize, Optional<ByteBuffer> pagingState) {
    int offset =
        pagingState
            .map(
                buf -> {
                  int magic = buf.getInt();
                  assertThat(magic).isEqualTo(MAGIC);
                  return buf.getInt();
                })
            .orElse(0);

    return new ValidatingPaginator(pageSize, offset);
  }

  public <T> List<T> filter(List<T> data) {
    int from = Math.max(offset, 0);
    if (from >= data.size()) {
      return Collections.emptyList();
    }

    int to = from + pageSize;
    if (to > data.size()) {
      to = data.size();
    } else {
      // Emulate C* behaviour - if the requested page ends exactly on or before the known data
      // boundary we will return a non-null paging state because the data set is not technically
      // "exhausted" yet. Only when we try and fail to fetch more data than available, we'll flag
      // "end of data" by returning an empty paging state
      nextOffset = to;
    }

    return data.subList(from, to);
  }

  public ByteBuffer pagingState() {
    if (nextOffset < 0) {
      return null;
    }

    return pagingState(nextOffset);
  }

  public ByteBuffer pagingStateForRow(int index) {
    // for the row just current offset
    // + index of the row
    // + one to point to the next row
    return pagingState(offset + index + 1);
  }

  private static ByteBuffer pagingState(int offset) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putInt(MAGIC);
    buffer.putInt(offset);
    buffer.rewind();
    return buffer;
  }
}
