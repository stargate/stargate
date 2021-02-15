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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.graphql.GraphqlITBase;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
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

  @Test
  public void shouldGetFileWithSchema(@TestKeyspace CqlIdentifier keyspaceId) throws IOException {
    // given deployed schema
    Map<String, Object> response =
        executeGraphqlAdminQuery(createSchema(keyspaceId), "deploySchema");
    assertThat(response).isNotNull();
    String deployedVersion = getDeployedVersion(response);
    assertThat(deployedVersion).isNotNull();

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
        .contains((String.format("%s-%s.graphql", keyspaceId.toString(), deployedVersion)));

    String responseBody = getSchemaResponse.body().string();
    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("type User");
    assertThat(responseBody).contains("type Query");
  }

  @Test
  public void shouldGetFileWithSchemaUsingVersionArgument(@TestKeyspace CqlIdentifier keyspaceId)
      throws IOException {
    // given deployed two schemas
    String schema = createSchema(keyspaceId);
    Map<String, Object> response = executeGraphqlAdminQuery(schema, "deploySchema");
    assertThat(response).isNotNull();
    String deployedVersion = getDeployedVersion(response);
    assertThat(deployedVersion).isNotNull();

    // when get schema file for the deployedVersion
    String url =
        String.format(
            "%s:8080/graphqlv2/files/namespace/%s.graphql?version=%s",
            host, keyspaceId, deployedVersion);
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
        .contains((String.format("%s-%s.graphql", keyspaceId.toString(), deployedVersion)));

    String responseBody = getSchemaResponse.body().string();
    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("type User");
    assertThat(responseBody).contains("type Query");
  }

  private String createSchema(CqlIdentifier keyspaceId) {
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
  private String getDeployedVersion(Map<String, Object> response) {
    Map<String, Object> deployedSchemaResponse = (Map<String, Object>) response.get("deploySchema");
    return (String) deployedSchemaResponse.get("version");
  }
}
