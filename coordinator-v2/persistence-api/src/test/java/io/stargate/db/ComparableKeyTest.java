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
package io.stargate.db;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ComparableKeyTest {

  @Test
  void same() {
    ComparableKey<String> key = new ComparableKey<>(String.class, "a");
    assertThat(key).isEqualTo(key);
    assertThat(key).isGreaterThanOrEqualTo(key);
    assertThat(key).isLessThanOrEqualTo(key);
  }

  @Test
  void equal() {
    ComparableKey<Integer> key1 = new ComparableKey<>(Integer.class, 2);
    ComparableKey<Integer> key2 = new ComparableKey<>(Integer.class, 1 + 1);
    assertThat(key1).isEqualTo(key2);
    assertThat(key2).isEqualTo(key1);
    assertThat(key1).isGreaterThanOrEqualTo(key2);
    assertThat(key1).isLessThanOrEqualTo(key2);
    assertThat(key2).isGreaterThanOrEqualTo(key1);
    assertThat(key2).isLessThanOrEqualTo(key1);

    assertThat(key1.hashCode()).isEqualTo(key2.hashCode());

    assertThat(key1.toString()).isEqualTo(key2.toString());
  }

  @Test
  void different() {
    ComparableKey<Integer> key1 = new ComparableKey<>(Integer.class, 1);
    ComparableKey<Integer> key2 = new ComparableKey<>(Integer.class, 2);
    assertThat(key1).isNotEqualTo(key2);
    assertThat(key1).isNotEqualByComparingTo(key2);
    assertThat(key2).isNotEqualTo(key1);
    assertThat(key2).isNotEqualByComparingTo(key1);

    assertThat(key1).isLessThan(key2);
    assertThat(key2).isGreaterThan(key1);
  }

  @Test
  void differentClass() {
    // This is not an expected use case, but it's included for the sake of coverage
    ComparableKey<?> key1 = new ComparableKey<>(String.class, "a");
    ComparableKey<?> key2 = new ComparableKey<>(Integer.class, 2);
    assertThat(key1).isNotEqualTo(key2);
    assertThat(key2).isNotEqualTo(key1);

    assertThat(key1.compareTo(key2)).isGreaterThan(0);
    assertThat(key2.compareTo(key1)).isLessThan(0);
  }

  @Test
  void notEqualToIrrelevantClass() {
    ComparableKey<String> key = new ComparableKey<>(String.class, "a");
    //noinspection EqualsBetweenInconvertibleTypes
    assertThat(key.equals("test")).isFalse();
  }
}
