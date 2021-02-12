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
package io.stargate.graphql.web.resources.schemafirst;

import graphql.GraphQL;
import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path(ResourcePaths.FILES)
public class FilesResource extends GraphqlResourceBase {

  private final SchemaFirstCache schemaFirstCache;

  @Inject
  public FilesResource(SchemaFirstCache schemaFirstCache) {
    this.schemaFirstCache = schemaFirstCache;
  }

  @GET
  @Path("/cql_directives.graphql")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getCqlDirectives() {
    return Response.ok(loadCqlDirectivesFile())
        .header("Content-Disposition", "inline; filename=\"cql_directives.graphql\"")
        .build();
  }

  // graphqlv2/files/namespace/{namespaceName}.graphql[?version={uuid}]
  @GET
  @Path("/namespace/{namespaceName}.graphql")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getSchema(
      @PathParam("namespaceName") String namespace,
      @QueryParam("version") String version,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse)
      throws Exception {
    GraphQL graphql =
        getGraphql(
            namespace,
            httpRequest,
            asyncResponse,
            schemaFirstCache,
            Optional.ofNullable(version).map(UUID::fromString));

    if (graphql != null) {
      // todo what to do with missing parameters?
      get(query, operationName, variables, graphql, httpRequest, asyncResponse);
    }
    return Response.ok().build();
  }

  private InputStreamReader loadCqlDirectivesFile() {
    return new InputStreamReader(
        this.getClass().getResourceAsStream("/schemafirst/cql_directives.graphql"),
        StandardCharsets.UTF_8);
  }
}
