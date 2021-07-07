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
package io.stargate.web.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.datastore.Row;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RawDocumentTest {

  private final String id = "testId";
  private final List<String> key = ImmutableList.of("1", "2");

  private final Function<ResumeMode, ByteBuffer> somePagingState =
      resumeMode -> ByteBuffer.allocate(1);
  @Mock private Function<ResumeMode, ByteBuffer> nullPagingState;
  @Mock private Row row1;
  @Mock private Row row2;
  private List<Row> rows;

  @BeforeEach
  void setup() {
    rows = ImmutableList.of(row1, row2);
  }

  @Nested
  class PagingState {
    @Test
    void testNullPagingState() {
      RawDocument doc = new RawDocument(id, key, nullPagingState, ImmutableList.of());
      assertThat(doc.hasPagingState()).isFalse();
      assertThat(doc.makePagingState()).isNull();
    }

    @Test
    void testSomePagingState() {
      RawDocument doc = new RawDocument(id, key, somePagingState, ImmutableList.of());
      assertThat(doc.hasPagingState()).isTrue();
      assertThat(doc.makePagingState()).isNotNull();
    }
  }

  @Nested
  class Populate {
    @Test
    void testRowsReplacement() {
      RawDocument doc0 = new RawDocument(id, key, somePagingState, ImmutableList.of());
      RawDocument doc1 = new RawDocument(id, key, nullPagingState, rows);

      List<RawDocument> docs = doc0.populateFrom(Flowable.just(doc1)).test().values();
      assertThat(docs).hasSize(1);
      assertThat(docs).element(0).extracting(RawDocument::id).isEqualTo(id);
      assertThat(docs).element(0).extracting(RawDocument::key).isEqualTo(key);
      assertThat(docs).element(0).extracting(RawDocument::rows).isSameAs(rows);
      assertThat(docs).element(0).extracting(RawDocument::hasPagingState).isEqualTo(true);
    }

    @Test
    void testEmptyRows() {
      RawDocument doc0 = new RawDocument(id, key, somePagingState, rows);
      RawDocument doc1 = new RawDocument(id, key, nullPagingState, ImmutableList.of());

      List<RawDocument> docs = doc0.populateFrom(Flowable.just(doc1)).test().values();
      assertThat(docs).hasSize(1);
      assertThat(docs).element(0).extracting(RawDocument::id).isEqualTo(id);
      assertThat(docs).element(0).extracting(RawDocument::key).isEqualTo(key);
      assertThat(docs).element(0).extracting(RawDocument::rows).asList().isEmpty();
      // the original paging state gets reused
      assertThat(docs).element(0).extracting(RawDocument::hasPagingState).isEqualTo(true);
    }

    @Test
    void testEmptyFlowable() {
      RawDocument doc0 = new RawDocument(id, key, somePagingState, rows);

      List<RawDocument> docs = doc0.populateFrom(Flowable.empty()).test().values();
      assertThat(docs).hasSize(1);
      assertThat(docs).element(0).extracting(RawDocument::id).isEqualTo(id);
      assertThat(docs).element(0).extracting(RawDocument::key).isEqualTo(key);
      // Populating from an empty result sets yields the same doc ID but no data rows
      assertThat(docs).element(0).extracting(RawDocument::rows).asList().isEmpty();
      // the original paging state gets reused
      assertThat(docs).element(0).extracting(RawDocument::hasPagingState).isEqualTo(true);
    }
  }
}
