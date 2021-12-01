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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurferJackson;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ShredderJmhTest {

  private String json =
      "{\"_id\":\"61a7681abde9da9471c4b96f\",\"index\":0,\"guid\":\"232f7a49-afde-4733-af4b-f2ff475c1088\",\"isActive\":false,\"balance\":\"$2,206.66\",\"picture\":\"http://placehold.it/32x32\",\"age\":22,\"eyeColor\":\"brown\",\"name\":\"Poole Barry\",\"gender\":\"male\",\"company\":\"LINGOAGE\",\"email\":\"poolebarry@lingoage.com\",\"phone\":\"+1 (809) 541-3100\",\"address\":\"403 Myrtle Avenue, Chaparrito, Pennsylvania, 2418\",\"about\":\"Proident sunt laborum qui voluptate mollit quis esse incididunt reprehenderit occaecat laborum. Irure magna qui excepteur sunt enim cillum ad incididunt anim aliquip mollit nulla commodo dolore. Lorem sit cillum officia duis id velit nisi culpa aliqua laborum. Magna tempor voluptate eu tempor culpa nulla elit labore Lorem sit magna adipisicing. Esse nisi elit eu duis. Nisi in labore elit exercitation laborum velit ex deserunt. Veniam ea reprehenderit ipsum sit exercitation cupidatat elit aute nisi.\\r\\n\",\"registered\":\"2018-01-02T06:55:08 -01:00\",\"latitude\":19.75239,\"longitude\":-115.309104,\"tags\":[\"laboris\",\"amet\",\"nulla\",\"aliqua\",\"voluptate\",\"tempor\",\"sunt\"],\"friends\":[{\"id\":0,\"name\":\"Barker Simon\"},{\"id\":1,\"name\":\"Jennie West\"},{\"id\":2,\"name\":\"Sophie Wade\"}],\"greeting\":\"Hello, Poole Barry! You have 6 unread messages.\",\"favoriteFruit\":\"apple\"}";

  private JsonDocumentShredder jsonDocumentShredder;

  private DocsShredder docsShredder;

  @Setup(Level.Trial)
  public void setup() {
    ObjectMapper objectMapper = new ObjectMapper();
    jsonDocumentShredder = new JsonDocumentShredder(DocsApiConfiguration.DEFAULT, objectMapper);
    docsShredder = new DocsShredder(DocsApiConfiguration.DEFAULT);
  }

  @Benchmark
  public void jsonShredder(Blackhole blackhole) {
    List<JsonShreddedRow> result = jsonDocumentShredder.shred(json, Collections.emptyList());
    blackhole.consume(result);
  }

  @Benchmark
  public void docShredder(Blackhole blackhole) {
    ImmutablePair<List<Object[]>, List<String>> result =
        docsShredder.shredPayload(
            JsonSurferJackson.INSTANCE, Collections.emptyList(), "key", json, false, false, true);
    blackhole.consume(result);
  }
}
