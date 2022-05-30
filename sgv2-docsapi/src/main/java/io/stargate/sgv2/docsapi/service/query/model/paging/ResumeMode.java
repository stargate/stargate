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

/** Client side reference of the resume mode. */
public enum ResumeMode {

  // TODO check if this will be part of the grpc api

  /**
   * Paging is resumed from the first row of the partition following the partition of the reference
   * row.
   */
  NEXT_PARTITION,

  /** Paging is resumed from the row following the reference row. */
  NEXT_ROW,
}
