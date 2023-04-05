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
package io.stargate.graphql.web.resources;

import io.dropwizard.util.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.osgi.framework.Bundle;

@Produces(MediaType.TEXT_HTML)
@Path("/playground")
@Singleton
public class PlaygroundResource {

  private final String playgroundFile;

  @Inject
  public PlaygroundResource(Bundle bundle) throws IOException {
    // From
    // https://raw.githubusercontent.com/prisma-labs/graphql-playground/master/packages/graphql-playground-html/withAnimation.html
    URL entry = bundle.getEntry("/playground.html");
    // Save the templated file away for later so that we only have to do this conversion once.
    playgroundFile =
        new BufferedReader(
                new InputStreamReader(
                    entry.openConnection().getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
  }

  @GET
  public Response get(@Context HttpServletRequest request) {
    String token = request.getHeader("x-cassandra-token");

    if (Strings.isNullOrEmpty(token)) {
      token = request.getHeader("Authorization");
      token = Strings.isNullOrEmpty(token) ? token : token.replaceFirst("^Bearer\\s", "");
    }

    // Replace the templated text with the token if it exist. Using java.lang.String.replaceFirst
    // since it's safer than java.lang.String.format(java.lang.String, java.lang.Object...) due to
    // the percent signs that exist in the string.
    String formattedPlaygroundFile =
        playgroundFile.replaceFirst("AUTHENTICATION_TOKEN", token == null ? "" : token);

    return Response.ok(formattedPlaygroundFile).build();
  }
}
