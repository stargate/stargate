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
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.graphql.config.GraphQLConfig;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

@Produces(MediaType.TEXT_HTML)
@Path("/playground")
@Singleton
public class PlaygroundResource {

  private final TokenSupplier tokenSupplier;

  private final Optional<String> playgroundFile;

  public PlaygroundResource(StargateRequestInfo requestInfo, GraphQLConfig config)
      throws IOException {
    this.tokenSupplier = new TokenSupplier(requestInfo, config);

    // if enabled get the HTML
    if (config.playground().enabled()) {
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
        .map(html -> serve(html, tokenSupplier.apply(headers)))
        .orElse(Response.status(Response.Status.NOT_FOUND).build());
  }

  private Response serve(String html, String token) {
    // Replace the templated text with the token if it exist. Using java.lang.String.replaceFirst
    // since it's safer than java.lang.String.format(java.lang.String, java.lang.Object...) due to
    // the percent signs that exist in the string.
    String formattedPlaygroundFile = html.replaceFirst("AUTHENTICATION_TOKEN", token);

    return Response.ok(formattedPlaygroundFile).build();
  }

  // simple function that can resolve token used in the playground
  record TokenSupplier(StargateRequestInfo requestInfo, GraphQLConfig config)
      implements Function<HttpHeaders, String> {

    @Override
    public String apply(HttpHeaders httpHeaders) {
      // first go for the request info
      return requestInfo
          .getCassandraToken()
          .orElseGet(
              () -> {

                // if not available check if header to look in exists
                // if not, empty string
                return config
                    .playground()
                    .tokenHeader()
                    .flatMap(header -> Optional.ofNullable(httpHeaders.getHeaderString(header)))
                    .orElse("");
              });
    }
  }
}
