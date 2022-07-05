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
package io.stargate.sgv2.graphql.web.resources;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Produces(MediaType.TEXT_HTML)
@Path("/playground")
@Singleton
public class PlaygroundResource {

  private final Optional<String> playgroundFile;

  public PlaygroundResource(
      @ConfigProperty(name = "stargate.graphql.enable-playground") boolean enabled)
      throws IOException {
    if (enabled) {
      // Save the templated file away for later so that we only have to do this conversion once.
      URL url = Resources.getResource("playground.html");
      playgroundFile = Optional.of(Resources.toString(url, StandardCharsets.UTF_8));
    } else {
      playgroundFile = Optional.empty();
    }
  }

  @GET
  public Response get(@Context HttpHeaders headers) {
    return playgroundFile
        .map(html -> serve(html, getToken(headers)))
        .orElse(Response.status(Response.Status.NOT_FOUND).build());
  }

  private Response serve(String html, String token) {
    // Replace the templated text with the token if it exist. Using java.lang.String.replaceFirst
    // since it's safer than java.lang.String.format(java.lang.String, java.lang.Object...) due to
    // the percent signs that exist in the string.
    String formattedPlaygroundFile =
        html.replaceFirst("AUTHENTICATION_TOKEN", token == null ? "" : token);

    return Response.ok(formattedPlaygroundFile).build();
  }

  private String getToken(HttpHeaders headers) {
    List<String> tokenHeaders = headers.getRequestHeader("x-cassandra-token");
    if (!tokenHeaders.isEmpty()) {
      return tokenHeaders.get(0);
    }

    List<String> authorizationHeaders = headers.getRequestHeader("Authorization");
    if (!authorizationHeaders.isEmpty()) {
      return authorizationHeaders.get(0).replaceFirst("^Bearer\\s", "");
    }

    return null;
  }
}
