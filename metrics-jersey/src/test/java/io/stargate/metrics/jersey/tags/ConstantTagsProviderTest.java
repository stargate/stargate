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

package io.stargate.metrics.jersey.tags;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ConstantTagsProviderTest {

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() {
      Tags tags = Tags.of("one", "two");

      ConstantTagsProvider provider = new ConstantTagsProvider(tags);
      Iterable<Tag> result = provider.httpRequestTags(null);

      assertThat(result).isEqualTo(tags);
    }
  }

  @Nested
  class HttpLongRequestTags {

    @Test
    public void happyPath() {
      Tags tags = Tags.of("one", "two");

      ConstantTagsProvider provider = new ConstantTagsProvider(tags);
      Iterable<Tag> result = provider.httpLongRequestTags(null);

      assertThat(result).isEqualTo(tags);
    }
  }
}
