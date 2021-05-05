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

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.google.common.io.Resources;
import graphql.schema.idl.SchemaPrinter;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.graphql.schema.graphqlfirst.AdminSchemaBuilder;
import io.stargate.graphql.schema.scalars.CqlScalar;
import io.stargate.graphql.web.RequestToHeadersMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A REST service that provides download links for some of the results returned by the admin API.
 *
 * @see AdminSchemaBuilder
 * @see AdminResource
 */
@Singleton
@Path(ResourcePaths.FILES)
@Authenticated
public class FilesResource {

  private static final Logger LOG = LoggerFactory.getLogger(FilesResource.class);
  private static final String DIRECTIVES_RESPONSE = buildDirectivesResponse();

  private final SchemaSourceDao schemaSourceDao;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;

  @Inject
  public FilesResource(
      SchemaSourceDao schemaSourceDao,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    this.schemaSourceDao = schemaSourceDao;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
  }

  @GET
  @Path("/cql_directives.graphql")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getCqlDirectives() {
    return Response.ok(DIRECTIVES_RESPONSE)
        .header("Content-Disposition", "inline; filename=\"cql_directives.graphql\"")
        .build();
  }

  @GET
  @Path("/keyspace/{keyspaceName}.graphql")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSchema(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") String keyspace,
      @QueryParam("version") String version,
      @Context HttpServletRequest httpRequest)
      throws Exception {

    if (!DmlResource.KEYSPACE_NAME_PATTERN.matcher(keyspace).matches()) {
      LOG.warn("Invalid keyspace in URI, this could be an XSS attack: {}", keyspace);
      // Do not reflect back the value
      return Response.status(Response.Status.BAD_REQUEST).entity("Invalid keyspace name").build();
    }

    if (!isAuthorized(token, httpRequest)) {
      return Response.status(Response.Status.UNAUTHORIZED).build();
    }

    UUID versionUuid = null;
    if (version != null) {
      try {
        versionUuid = UUID.fromString(version);
      } catch (IllegalArgumentException e) {
        return notFound(keyspace, version);
      }
    }

    SchemaSource schemaSource =
        schemaSourceDao.getSingleVersion(keyspace, Optional.ofNullable(versionUuid));
    if (schemaSource == null) {
      return notFound(keyspace, version);
    }

    return Response.ok(schemaSource.getContents())
        .header("Content-Disposition", "inline; filename=" + createFileName(schemaSource))
        .build();
  }

  private Response notFound(String keyspace, String version) {
    return Response.status(
            Response.Status.NOT_FOUND.getStatusCode(),
            String.format(
                "The schema for keyspace %s and version %s does not exist.", keyspace, version))
        .build();
  }

  private String createFileName(SchemaSource schemaSource) {
    return String.format(
        "\"%s-%s.graphql\"", schemaSource.getKeyspace(), schemaSource.getVersion());
  }

  private static String buildDirectivesResponse() {
    try {
      StringBuilder result =
          new StringBuilder(
              Resources.toString(
                  Resources.getResource(FilesResource.class, "/schemafirst/cql_directives.graphql"),
                  Charsets.UTF_8));

      result.append('\n');
      SchemaPrinter schemaPrinter = new SchemaPrinter();
      for (CqlScalar cqlScalar : CqlScalar.values()) {
        result.append(schemaPrinter.print(cqlScalar.getGraphqlType()));
      }
      return result.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean isAuthorized(String token, HttpServletRequest httpRequest) {
    try {
      Map<String, String> headers = RequestToHeadersMapper.getAllHeaders(httpRequest);
      AuthenticationSubject authenticationSubject =
          authenticationService.validateToken(token, headers);
      authorizationService.authorizeSchemaRead(
          authenticationSubject,
          Collections.singletonList(SchemaSourceDao.KEYSPACE_NAME),
          Collections.singletonList(SchemaSourceDao.TABLE_NAME),
          SourceAPI.GRAPHQL,
          ResourceKind.TABLE);
      return true;
    } catch (UnauthorizedException e) {
      return false;
    }
  }
}
