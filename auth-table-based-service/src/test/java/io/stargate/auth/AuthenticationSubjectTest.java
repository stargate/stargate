package io.stargate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.AuthenticatedUser.Serializer;
import java.nio.ByteBuffer;
import java.util.Collections;
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
class AuthenticationSubjectTest {
  @Test
  public void serializationRoundTripMinimal() {
    AuthenticatedUser user = AuthenticatedUser.of("name1", "token2");
    Map<String, ByteBuffer> bytes = Serializer.serialize(user);
    AuthenticationSubject subject = AuthenticationSubject.of(Serializer.load(bytes));

    assertThat(subject.roleName()).isEqualTo("name1");
    assertThat(subject.token()).isEqualTo("token2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(false);
    assertThat(subject.customProperties()).isEmpty();
  }

  @Test
  public void serializationRoundTripWithCustom() {
    AuthenticatedUser user =
        AuthenticatedUser.of(
            "name1", "token2", true, ImmutableMap.of("key1", "val1", "key2", "val2"));
    Map<String, ByteBuffer> bytes = Serializer.serialize(user);
    AuthenticationSubject subject = AuthenticationSubject.of(Serializer.load(bytes));

    assertThat(subject.roleName()).isEqualTo("name1");
    assertThat(subject.token()).isEqualTo("token2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(true);
    assertThat(subject.customProperties())
        .containsExactlyEntriesOf(ImmutableMap.of("key1", "val1", "key2", "val2"));
  }

  @Test
  public void minimalInputs() {
    AuthenticationSubject subject = AuthenticationSubject.of("token1", "user2");
    assertThat(subject.token()).isEqualTo("token1");
    assertThat(subject.roleName()).isEqualTo("user2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(false);
    assertThat(subject.customProperties()).isEmpty();
  }

  @Test
  public void fullInputsLocal() {
    AuthenticationSubject subject =
        AuthenticationSubject.of(
            "token1", "user2", false, ImmutableMap.of("key1", "val1", "key2", "val2"));
    assertThat(subject.token()).isEqualTo("token1");
    assertThat(subject.roleName()).isEqualTo("user2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(false);
    assertThat(subject.customProperties())
        .containsExactlyEntriesOf(ImmutableMap.of("key1", "val1", "key2", "val2"));
  }

  @Test
  public void fullInputsExternal() {
    AuthenticationSubject subject =
        AuthenticationSubject.of("token1", "user2", true, Collections.emptyMap());
    assertThat(subject.token()).isEqualTo("token1");
    assertThat(subject.roleName()).isEqualTo("user2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(true);
    assertThat(subject.customProperties()).isEmpty();
  }

  @Test
  public void fullInputsExternalWithNoCustomProperties() {
    AuthenticationSubject subject = AuthenticationSubject.of("token1", "user2", true);
    assertThat(subject.token()).isEqualTo("token1");
    assertThat(subject.roleName()).isEqualTo("user2");
    assertThat(subject.isFromExternalAuth()).isEqualTo(true);
    assertThat(subject.customProperties()).isEmpty();
  }

  @Test
  public void convertToAuthenticatedUser() {
    AuthenticatedUser user =
        AuthenticationSubject.of("token1", "user2", true, ImmutableMap.of("p1", "v1")).asUser();
    assertThat(user.token()).isEqualTo("token1");
    assertThat(user.name()).isEqualTo("user2");
    assertThat(user.isFromExternalAuth()).isEqualTo(true);
    assertThat(user.customProperties()).isEqualTo(ImmutableMap.of("p1", "v1"));
  }
}
