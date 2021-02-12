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

import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path(ResourcePaths.FILES)
@Produces(MediaType.APPLICATION_JSON)
public class FilesResource extends GraphqlResourceBase {

  @GET
  @Path("/cql_directives.graphql")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getCqlDirectives() {
    return Response.ok(loadCqlDirectivesFile()).build();
  }

  private InputStreamReader loadCqlDirectivesFile() {
    return new InputStreamReader(
        this.getClass().getResourceAsStream("/schemafirst/cql_directives.graphql"),
        StandardCharsets.UTF_8);
  }
}
