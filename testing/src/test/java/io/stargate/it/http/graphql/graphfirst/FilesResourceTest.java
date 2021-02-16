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
package io.stargate.it.http.graphql.graphfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.graphql.GraphqlTestBase;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class FilesResourceTest extends GraphqlTestBase {
  private static String DEPLOYED_SCHEMA_VERSION;

  @BeforeAll
  public static void deploySchema(@TestKeyspace CqlIdentifier keyspaceId) {
    // deploySchema
    Map<String, Object> response =
        executeGraphqlAdminQuery(createSchema(keyspaceId), "deploySchema");
    assertThat(response).isNotNull();
    DEPLOYED_SCHEMA_VERSION = getDeployedVersion(response);
    assertThat(DEPLOYED_SCHEMA_VERSION).isNotNull();
  }

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

  @Test
  public void shouldGetFileWithSchema(@TestKeyspace CqlIdentifier keyspaceId) throws IOException {
    // when get schema file
    String url = String.format("%s:8080/graphqlv2/files/namespace/%s.graphql", host, keyspaceId);
    Request getRequest =
        new Request.Builder()
            .get()
            .addHeader("content-type", MediaType.TEXT_PLAIN)
            .url(url)
            .build();

    Response getSchemaResponse = getHttpClient().newCall(getRequest).execute();

    // then
    assertThat(getSchemaResponse.code()).isEqualTo(200);
    assertThat(getSchemaResponse.header("Content-Disposition"))
        .contains((String.format("%s-%s.graphql", keyspaceId.toString(), DEPLOYED_SCHEMA_VERSION)));

    String responseBody = getSchemaResponse.body().string();
    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("type User");
    assertThat(responseBody).contains("type Query");
  }

  @Test
  public void shouldGetFileWithSchemaUsingVersionArgument(@TestKeyspace CqlIdentifier keyspaceId)
      throws IOException {
    // when get schema file for the deployedVersion
    String url =
        String.format(
            "%s:8080/graphqlv2/files/namespace/%s.graphql?version=%s",
            host, keyspaceId, DEPLOYED_SCHEMA_VERSION);
    Request getRequest =
        new Request.Builder()
            .get()
            .addHeader("content-type", MediaType.TEXT_PLAIN)
            .url(url)
            .build();

    Response getSchemaResponse = getHttpClient().newCall(getRequest).execute();

    // then
    assertThat(getSchemaResponse.code()).isEqualTo(200);
    assertThat(getSchemaResponse.header("Content-Disposition"))
        .contains((String.format("%s-%s.graphql", keyspaceId.toString(), DEPLOYED_SCHEMA_VERSION)));

    String responseBody = getSchemaResponse.body().string();
    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("type User");
    assertThat(responseBody).contains("type Query");
  }

  @Test
  public void shouldReturnNotFoundIfSchemaDoesNotExists() throws IOException {
    // when get schema file
    String url = String.format("%s:8080/graphqlv2/files/namespace/%s.graphql", host, "unknown");
    Request getRequest =
        new Request.Builder()
            .get()
            .addHeader("content-type", MediaType.TEXT_PLAIN)
            .url(url)
            .build();

    Response getSchemaResponse = getHttpClient().newCall(getRequest).execute();

    // then
    assertThat(getSchemaResponse.code()).isEqualTo(404);
  }

  @Test
  @DisplayName("Should return 404 if unauthorized")
  public void unauthorizedTest(@TestKeyspace CqlIdentifier keyspaceId) throws Exception {
    // when get schema file for the deployedVersion with the wrong token
    String url =
        String.format(
            "%s:8080/graphqlv2/files/namespace/%s.graphql?version=%s",
            host, keyspaceId, DEPLOYED_SCHEMA_VERSION);
    Request getRequest =
        new Request.Builder()
            .get()
            .addHeader("content-type", MediaType.TEXT_PLAIN)
            .url(url)
            .build();

    Response getSchemaResponse = getHttpClient("wrong auth token").newCall(getRequest).execute();

    // then
    assertThat(getSchemaResponse.code()).isEqualTo(404);
  }

  private static String createSchema(CqlIdentifier keyspaceId) {
    return String.format(
        "mutation {\n"
            + "  deploySchema(\n"
            + "    namespace: \"%s\"\n"
            + "    schema: \"\"\"\n"
            + "    type User {\n"
            + "      id: ID!\n"
            + "      name: String\n"
            + "      username: String\n"
            + "    }\n"
            + "    type Query {\n"
            + "      getUser(id: ID!): User\n"
            + "    }\n"
            + "    \"\"\"\n"
            + "  )\n"
            + "{\n"
            + "    version\n"
            + "  }\n"
            + "}\n",
        keyspaceId);
  }

  @SuppressWarnings("unchecked")
  private static String getDeployedVersion(Map<String, Object> response) {
    Map<String, Object> deployedSchemaResponse = (Map<String, Object>) response.get("deploySchema");
    return (String) deployedSchemaResponse.get("version");
  }
}
