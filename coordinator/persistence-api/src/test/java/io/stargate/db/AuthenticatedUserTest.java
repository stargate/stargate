package io.stargate.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.db.AuthenticatedUser.Serializer;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
class AuthenticatedUserTest {

  @Test
  public void serializationRoundTripMinimal() {
    AuthenticatedUser user0 = AuthenticatedUser.of("name1");

    assertThat(user0.name()).isEqualTo("name1");
    assertThat(user0.token()).isNull();
    assertThat(user0.isFromExternalAuth()).isEqualTo(false);
    assertThat(user0.customProperties()).isEmpty();

    Map<String, ByteBuffer> bytes = Serializer.serialize(user0);

    assertThatThrownBy(() -> Serializer.load(bytes))
        .hasMessage("token and roleName must be provided");
  }

  @Test
  public void serializationRoundTrip() {
    AuthenticatedUser user0 = AuthenticatedUser.of("name1", "token2");

    assertThat(user0.name()).isEqualTo("name1");
    assertThat(user0.token()).isEqualTo("token2");
    assertThat(user0.isFromExternalAuth()).isEqualTo(false);
    assertThat(user0.customProperties()).isEmpty();

    Map<String, ByteBuffer> bytes = Serializer.serialize(user0);
    AuthenticatedUser user1 = Serializer.load(bytes);

    assertThat(user1.name()).isEqualTo("name1");
    assertThat(user1.token()).isEqualTo("token2");
    assertThat(user1.isFromExternalAuth()).isEqualTo(false);
    assertThat(user1.customProperties()).isEmpty();
  }

  @Test
  public void serializationRoundTripWithCustom() {
    AuthenticatedUser user0 =
        AuthenticatedUser.of(
            "name1", "token2", true, ImmutableMap.of("key1", "val1", "key2", "val2"));

    assertThat(user0.name()).isEqualTo("name1");
    assertThat(user0.token()).isEqualTo("token2");
    assertThat(user0.isFromExternalAuth()).isEqualTo(true);
    assertThat(user0.customProperties())
        .containsExactlyEntriesOf(ImmutableMap.of("key1", "val1", "key2", "val2"));

    Map<String, ByteBuffer> bytes = Serializer.serialize(user0);
    AuthenticatedUser user1 = Serializer.load(bytes);

    assertThat(user1.name()).isEqualTo("name1");
    assertThat(user1.token()).isEqualTo("token2");
    assertThat(user1.isFromExternalAuth()).isEqualTo(true);
    assertThat(user1.customProperties())
        .containsExactlyEntriesOf(ImmutableMap.of("key1", "val1", "key2", "val2"));
  }
}
