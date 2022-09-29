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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class FilterPathTest {

  @Test
  public void emptyPath() {
    Throwable throwable = catchThrowable(() -> ImmutableFilterPath.of(Collections.emptyList()));
    assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void onlyField() {
    List<String> path = Collections.singletonList("field");

    FilterPath filterPath = ImmutableFilterPath.of(path);

    assertThat(filterPath.getPath()).isEqualTo(path);
    assertThat(filterPath.getField()).isEqualTo("field");
    assertThat(filterPath.getParentPath()).isEmpty();
    assertThat(filterPath.getPathString()).isEqualTo("field");
    assertThat(filterPath.getParentPathString()).isEqualTo("");
  }

  @Test
  public void nestedField() {
    List<String> path = Arrays.asList("path", "to", "field");

    FilterPath filterPath = ImmutableFilterPath.of(path);

    assertThat(filterPath.getPath()).isEqualTo(path);
    assertThat(filterPath.getField()).isEqualTo("field");
    assertThat(filterPath.getParentPath()).containsExactly("path", "to");
    assertThat(filterPath.getPathString()).isEqualTo("path.to.field");
    assertThat(filterPath.getParentPathString()).isEqualTo("path.to");
  }
}
