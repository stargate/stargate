package io.stargate.web.docsapi.resources;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.SchemalessRequestException;
import io.stargate.web.docsapi.service.SchemalessService;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.resources.Db;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class SchemalessResource {

  @Inject private Db dbFactory;
  private static final Logger logger = LoggerFactory.getLogger(SchemalessResource.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final SchemalessService schemalessService = new SchemalessService();

  @POST
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response postDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      String payload) {
    // This route does nearly the same thing as PUT, except that it assigns an ID for the requester
    // And returns it as a Location header/in JSON body
    logger.debug("Post: Collection = {}", collection);
    String newId = UUID.randomUUID().toString();
    return handle(
        () -> {
          schemalessService.putAtPath(
              authToken, keyspace, collection, newId, payload, new ArrayList<>(), false, dbFactory);
          ObjectNode node = mapper.createObjectNode();
          node.set("id", TextNode.valueOf(newId));
          return Response.created(
                  URI.create(String.format("/v1/%s/%s/%s", keyspace, collection, newId)))
              .entity(mapper.writeValueAsString(node))
              .build();
        });
  }

  @PUT
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response putDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      String payload) {
    logger.debug("Put: Collection = {}, id = {}", collection, id);

    return handle(
        () -> {
          schemalessService.putAtPath(
              authToken, keyspace, collection, id, payload, new ArrayList<>(), false, dbFactory);
          return Response.ok().build();
        });
  }

  @PUT
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response putDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @PathParam("path") List<PathSegment> path,
      String payload) {
    logger.debug("Put: Collection = {}, id = {}, path = {}", collection, id, path);

    return handle(
        () -> {
          schemalessService.putAtPath(
              authToken, keyspace, collection, id, payload, path, false, dbFactory);
          return Response.ok().build();
        });
  }

  @PATCH
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response patchDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      String payload) {
    logger.debug("Patch: Collection = {}, id = {}", collection, id);

    return handle(
        () -> {
          schemalessService.putAtPath(
              authToken, keyspace, collection, id, payload, new ArrayList<>(), true, dbFactory);
          return Response.ok().build();
        });
  }

  @PATCH
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response patchDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @PathParam("path") List<PathSegment> path,
      String payload) {
    logger.debug("Patch: Collection = {}, id = {}, path = {}", collection, id, path);

    return handle(
        () -> {
          schemalessService.putAtPath(
              authToken, keyspace, collection, id, payload, path, true, dbFactory);
          return Response.ok().build();
        });
  }

  @DELETE
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response deleteDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id) {
    logger.debug("Delete: Collection = {}, id = {}, path = {}", collection, id, new ArrayList<>());
    return handle(
        () -> {
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);
          schemalessService.deleteAtPath(db, keyspace, collection, id, new ArrayList<>());
          return Response.noContent().build();
        });
  }

  @DELETE
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response deleteDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @PathParam("path") List<PathSegment> path) {
    logger.debug("Delete: Collection = {}, id = {}, path = {}", collection, id, path);

    return handle(
        () -> {
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);
          schemalessService.deleteAtPath(db, keyspace, collection, id, path);
          return Response.noContent().build();
        });
  }

  @GET
  @Path("query/{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}")
  @Produces("application/json")
  public Response searchDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("path") List<PathSegment> path,
      @QueryParam("where") String where,
      @QueryParam("recurse") Boolean recurse,
      @QueryParam("documentKey") String documentKey)
      throws JsonProcessingException {
    if (recurse == null) {
      // If someone's searching on the doc's root path, they probably want a recursive search.
      recurse = true;
    }
    return searchDocPath(
        headers, ui, authToken, keyspace, collection, path, where, recurse, documentKey);
  }

  @GET
  @Path("query/{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response searchDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("path") List<PathSegment> path,
      @QueryParam("where") String where,
      @QueryParam("recurse") Boolean recurse,
      @QueryParam("documentKey") String documentKey) {
    return handle(
        () -> {
          JsonNode filterJson = mapper.readTree(where);
          List<FilterCondition> filters =
              schemalessService.convertToFilterOps(new ArrayList<>(), filterJson);

          if (filters.size() > 1) {
            Set<String> distinctFields =
                filters.stream().map(FilterCondition::getField).collect(Collectors.toSet());
            if (distinctFields.size() > 1) {
              throw new SchemalessRequestException(
                  String.format(
                      "Conditions across multiple fields are not yet supported (found: %s)",
                      distinctFields));
            }
          }

          if (filters.isEmpty()) {
            throw new SchemalessRequestException("No filters supplied in `where` parameter");
          }

          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);
          if (path.size() > db.MAX_DEPTH) {
            throw new SchemalessRequestException("Invalid request, maximum depth exceeded");
          }

          JsonNode docsResult =
              schemalessService.searchDocuments(
                  db, keyspace, collection, documentKey, filters, path, recurse);

          if (docsResult == null) {
            return Response.noContent().build();
          }

          String json = mapper.writeValueAsString(docsResult);

          logger.debug(json);
          return Response.ok(json).build();
        });
  }

  @GET
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response getDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id) {
    return getDocPath(headers, ui, authToken, keyspace, collection, id, new ArrayList<>());
  }

  @GET
  @Path("{keyspace: [a-zA-Z_0-9]+}/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response getDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @PathParam("path") List<PathSegment> path) {
    return handle(
        () -> {
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);
          JsonNode node = schemalessService.getJsonAtPath(db, keyspace, collection, id, path);

          if (node == null) {
            return Response.noContent().build();
          }

          String json = mapper.writeValueAsString(node);

          logger.debug(json);
          return Response.ok(json).build();
        });
  }

  static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (UnauthorizedException ue) {
      return Response.status(Response.Status.UNAUTHORIZED).build();
    } catch (SchemalessRequestException sre) {
      return Response.status(Response.Status.BAD_REQUEST).entity(sre.getLocalizedMessage()).build();
    } catch (NoNodeAvailableException e) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity("Internal connection to Cassandra closed")
          .build();
    } catch (Throwable t) {
      logger.error("Error when executing request", t);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(t.getLocalizedMessage())
          .build();
    }
  }
}
