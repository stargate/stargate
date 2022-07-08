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
package io.stargate.sgv2.docsapi.service.query.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentPropertyComparatorTest {

  @Mock DocumentProperty p1;

  @Mock DocumentProperty p2;

  @Nested
  class Compare {

    @Test
    public void onByteCompare() {
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(ByteString.copyFromUtf8("b"));

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Collections.singletonList("path"));

      assertThat(comparator.compare(p1, p2)).isLessThan(0);
      assertThat(comparator.compare(p2, p1)).isGreaterThan(0);
      verify(p1, times(2)).comparableKey();
      verify(p2, times(2)).comparableKey();
      verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void onByteCompareNull() {
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(null);

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Collections.singletonList("path"));

      assertThat(comparator.compare(p1, p2)).isLessThan(0);
      assertThat(comparator.compare(p2, p1)).isGreaterThan(0);
      verify(p1, times(2)).comparableKey();
      verify(p2, times(2)).comparableKey();
      verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void onKey() {
      String path = "path";
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p1.keyValue(path)).thenReturn("b");
      when(p2.keyValue(path)).thenReturn("c");

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Collections.singletonList(path));

      assertThat(comparator.compare(p1, p2)).isLessThan(0);
      assertThat(comparator.compare(p2, p1)).isGreaterThan(0);
      verify(p1, times(2)).comparableKey();
      verify(p1, times(2)).keyValue(path);
      verify(p2, times(2)).comparableKey();
      verify(p2, times(2)).keyValue(path);
      verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void onSecondKey() {
      String path1 = "path1";
      String path2 = "path2";
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p1.keyValue(path1)).thenReturn("b");
      when(p2.keyValue(path1)).thenReturn("b");
      when(p1.keyValue(path2)).thenReturn("c");
      when(p2.keyValue(path2)).thenReturn("d");

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Arrays.asList(path1, path2));

      assertThat(comparator.compare(p1, p2)).isLessThan(0);
      assertThat(comparator.compare(p2, p1)).isGreaterThan(0);
      verify(p1, times(2)).comparableKey();
      verify(p1, times(2)).keyValue(path1);
      verify(p1, times(2)).keyValue(path2);
      verify(p2, times(2)).comparableKey();
      verify(p2, times(2)).keyValue(path1);
      verify(p2, times(2)).keyValue(path2);
      verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void onKeyNull() {
      String path = "path";
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Collections.singletonList(path));

      assertThat(comparator.compare(p1, p2)).isZero();
      assertThat(comparator.compare(p2, p1)).isZero();
      verify(p1, times(2)).comparableKey();
      verify(p1, times(2)).keyValue(path);
      verify(p2, times(2)).comparableKey();
      verify(p2, times(2)).keyValue(path);
      verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void onKeyEmpty() {
      String path = "path";
      when(p1.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p2.comparableKey()).thenReturn(ByteString.copyFromUtf8("a"));
      when(p1.keyValue(path)).thenReturn("");
      when(p2.keyValue(path)).thenReturn("");

      DocumentPropertyComparator comparator =
          new DocumentPropertyComparator(Collections.singletonList(path));

      assertThat(comparator.compare(p1, p2)).isZero();
      assertThat(comparator.compare(p2, p1)).isZero();
      verify(p1, times(2)).comparableKey();
      verify(p1, times(2)).keyValue(path);
      verify(p2, times(2)).comparableKey();
      verify(p2, times(2)).keyValue(path);
      verifyNoMoreInteractions(p1, p2);
    }
  }
}
