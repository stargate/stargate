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

import java.nio.ByteBuffer;

/**
 * An abstraction layer for sources of a query paging state.
 *
 * <p>In general a paging state may come from:
 * <li>a byte buffer, which in turn is a paging state returned from the previous query execution
 *     that was not utilized during the current processing round and hence can be reused "as is"
 * <li>a row that was received from the current (fresh) result set page.
 */
public interface PagingStateSupplier {

  /** Constructs paging state. */
  ByteBuffer makePagingState();

  /** Creates a {@link PagingStateSupplier} for a pre-built, fixed paging state buffer. */
  static PagingStateSupplier fixed(ByteBuffer pagingState) {
    return () -> pagingState;
  }
}
