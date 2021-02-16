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

import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path(ResourcePaths.FILES)
public class FilesResource extends GraphqlResourceBase {

  private final SchemaSourceDao schemaSourceDao;

  @Inject
  public FilesResource(SchemaSourceDao schemaSourceDao) {
    this.schemaSourceDao = schemaSourceDao;
  }

  @GET
  @Path("/cql_directives.graphql")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getCqlDirectives() {
    return Response.ok(loadCqlDirectivesFile())
        .header("Content-Disposition", "inline; filename=\"cql_directives.graphql\"")
        .build();
  }

  @GET
  @Path("/namespace/{namespaceName}.graphql")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSchema(
      @PathParam("namespaceName") String namespace,
      @QueryParam("version") String version,
      @Context HttpServletRequest httpRequest)
      throws Exception {
    SchemaSource schemaSource =
        schemaSourceDao.getByVersion(namespace, Optional.ofNullable(version).map(UUID::fromString));
    if (schemaSource == null) {
      return Response.status(
              Response.Status.NOT_FOUND.getStatusCode(),
              String.format(
                  "The schema for namespace: %s and version %s does not exists.",
                  namespace, version))
          .build();
    }

    return Response.ok(schemaSource.getContents())
        .header("Content-Disposition", "inline; filename=" + createFileName(schemaSource))
        .build();
  }

  private String createFileName(SchemaSource schemaSource) {
    return String.format(
        "\"%s-%s.graphql\"", schemaSource.getNamespace(), schemaSource.getVersion());
  }

  private InputStreamReader loadCqlDirectivesFile() {
    return new InputStreamReader(
        this.getClass().getResourceAsStream("/schemafirst/cql_directives.graphql"),
        StandardCharsets.UTF_8);
  }
}
