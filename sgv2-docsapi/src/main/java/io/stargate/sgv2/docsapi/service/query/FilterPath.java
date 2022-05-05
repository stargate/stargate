/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.sgv2.docsapi.service.query;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

/** Contains path information for a filter operation. */
@Value.Immutable
public interface FilterPath {

  /** @return The complete path to the field, including the field name as the last element. */
  @Value.Parameter
  List<String> getPath();

  @Value.Check
  default void check() {
    List<String> fullPath = getPath();
    if (null == fullPath || fullPath.isEmpty()) {
      throw new IllegalArgumentException("Filter path to a field must not be empty.");
    }
  }

  /**
   * @return Returns if the given path is fixed, meaning it does not contain any wildcards (globs).
   */
  @Value.Lazy
  default boolean isFixed() {
    return getParentPath().stream()
        .noneMatch(
            p ->
                Objects.equals(p, DocsApiConstants.GLOB_VALUE)
                    || Objects.equals(p, DocsApiConstants.GLOB_ARRAY_VALUE));
  }

  /** @return The name of the field. Effectively, the last element of {@link #getPath()}. */
  default String getField() {
    List<String> path = getPath();
    int length = path.size();
    return path.get(length - 1);
  }

  /** @return The parent path without the field. */
  default List<String> getParentPath() {
    List<String> path = getPath();
    if (path.size() > 1) {
      int length = path.size();
      return path.subList(0, length - 1);
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * @return Joins {@link #getParentPath()} with dot. Returns empty string if there are no paths.
   */
  default String getParentPathString() {
    return String.join(".", getParentPath());
  }

  /** @return Returns complete JSON path as String to the field, including the field. */
  default String getPathString() {
    return String.join(".", getPath());
  }
}
