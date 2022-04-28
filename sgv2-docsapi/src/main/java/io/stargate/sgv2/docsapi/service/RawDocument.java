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
package io.stargate.sgv2.docsapi.service;

import io.stargate.sgv2.docsapi.model.RowWrapper;
import java.util.List;

public class RawDocument {

  private final List<RowWrapper> rows;

  public RawDocument(List<RowWrapper> rows) {
    this.rows = rows;
  }

  public List<RowWrapper> rows() {
    return rows;
  }
}
