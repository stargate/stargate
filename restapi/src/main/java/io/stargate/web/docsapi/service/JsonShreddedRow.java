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

package io.stargate.web.docsapi.service;

import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface JsonShreddedRow {

  // TODO Convert to abstract class, hide?

  @Value.Check
  default void validate() {
    if (getPath().size() > getMaxDepth()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }
  }

  String getKey();

  int getMaxDepth();

  List<String> getPath();

  // leaf is always actually the last path we have
  default String getLeaf() {
    List<String> path = getPath();
    if (path != null && !path.isEmpty()) {
      return path.get(path.size() - 1);
    } else {
      return null; // TODO is this fine? what do we return as leaf for empty objects and empty
      // arrays
    }
  }

  @Nullable
  String getStringValue();

  @Nullable
  Double getDoubleValue();

  // booleans can be int or bools
  @Nullable
  Object getBooleanValue();
}
