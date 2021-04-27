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

package io.stargate.web.docsapi.service.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class FilterPathTest {

  @Test
  public void onlyField() {
    List<String> path = Collections.singletonList("field");

    FilterPath filterPath = ImmutableFilterPath.of(path);

    assertThat(filterPath.getFullPath()).isEqualTo(path);
    assertThat(filterPath.getField()).isEqualTo("field");
    assertThat(filterPath.getPath()).isEmpty();
    assertThat(filterPath.getFullFieldPathString()).isEqualTo("field");
    assertThat(filterPath.getPathString()).isEqualTo("");
  }

  @Test
  public void nestedField() {
    List<String> path = Arrays.asList("path", "to", "field");

    FilterPath filterPath = ImmutableFilterPath.of(path);

    assertThat(filterPath.getFullPath()).isEqualTo(path);
    assertThat(filterPath.getField()).isEqualTo("field");
    assertThat(filterPath.getPath()).containsExactly("path", "to");
    assertThat(filterPath.getFullFieldPathString()).isEqualTo("path.to.field");
    assertThat(filterPath.getPathString()).isEqualTo("path.to");
  }

  @Test
  public void empty() {
    List<String> path = Collections.emptyList();

    FilterPath filterPath = ImmutableFilterPath.of(path);

    assertThat(filterPath.getFullPath()).isEqualTo(path);
    assertThat(filterPath.getField()).isNull();
    assertThat(filterPath.getPath()).isNullOrEmpty();
    assertThat(filterPath.getFullFieldPathString()).isBlank();
    assertThat(filterPath.getPathString()).isBlank();
  }
}
