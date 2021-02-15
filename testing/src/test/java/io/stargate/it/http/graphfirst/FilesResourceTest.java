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
package io.stargate.it.http.graphfirst;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.http.graphql.GraphqlITBase;
import java.io.IOException;
import javax.ws.rs.core.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

public class FilesResourceTest extends GraphqlITBase {

  @Test
  public void shouldGetGraphQLDirectivesFile() throws IOException {
    // given
    OkHttpClient httpClient = getHttpClient();
    String url = String.format("%s:8080/graphqlv2/files/cql_directives.graphql", host);
    Request getRequest =
        new Request.Builder()
            .get()
            .addHeader("content-type", MediaType.TEXT_PLAIN)
            .url(url)
            .build();

    // when
    Response response = httpClient.newCall(getRequest).execute();

    // then
    assertThat(response.code()).isEqualTo(200);
    assertThat(response.body()).isNotNull();
    String responseBody = response.body().string();
    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("directive @cql_input");
  }
}
