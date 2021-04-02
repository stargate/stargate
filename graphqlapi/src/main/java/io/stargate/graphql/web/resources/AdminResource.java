package io.stargate.graphql.web.resources;

import graphql.GraphQL;
import io.stargate.graphql.web.models.GraphqlJsonBody;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 * A GraphQL service that allows users to deploy and manage custom GraphQL schemas for their
 * keyspaces.
 */
@Singleton
@Path(ResourcePaths.ADMIN)
@Produces(MediaType.APPLICATION_JSON)
@Authenticated
public class AdminResource extends GraphqlResourceBase {

  private final GraphQL graphql;

  @Inject
  public AdminResource(GraphqlCache graphqlCache) {
    this.graphql = graphqlCache.getSchemaFirstAdminGraphql();
  }

  @GET
  public void get(
      @QueryParam("query") String query,
      @QueryParam("operationName") String operationName,
      @QueryParam("variables") String variables,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    get(query, operationName, variables, graphql, httpRequest, asyncResponse);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void postJson(
      GraphqlJsonBody jsonBody,
      @QueryParam("query") String queryFromUrl,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    postJson(jsonBody, queryFromUrl, graphql, httpRequest, asyncResponse);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void postMultipartJson(
      @FormDataParam("operations") GraphqlJsonBody graphqlPart,
      FormDataMultiPart allParts,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    postMultipartJson(graphqlPart, allParts, graphql, httpRequest, asyncResponse);
  }

  @POST
  @Consumes(APPLICATION_GRAPHQL)
  public void postGraphql(
      String query,
      @Context HttpServletRequest httpRequest,
      @Suspended AsyncResponse asyncResponse) {

    postGraphql(query, graphql, httpRequest, asyncResponse);
  }
}
