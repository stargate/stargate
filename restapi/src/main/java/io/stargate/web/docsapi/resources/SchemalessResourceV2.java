package io.stargate.web.docsapi.resources;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v2/")
@Produces(MediaType.APPLICATION_JSON)
public class SchemalessResourceV2 {
  @Inject private Db dbFactory;
  private static final Logger logger = LoggerFactory.getLogger(SchemalessResourceV2.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final SchemalessService schemalessService = new SchemalessService();
  private final int DEFAULT_PAGE_SIZE = 100;

  private JsonNode wrapResponse(JsonNode node, String id) {
    ObjectNode wrapperNode = mapper.createObjectNode();

    if (id != null) {
      wrapperNode.set("documentId", TextNode.valueOf(id));
    }
    if (node != null) {
      wrapperNode.set("data", node);
    }
    return wrapperNode;
  }

  private JsonNode wrapResponse(JsonNode node, String id, String pagingState) {
    ObjectNode wrapperNode = mapper.createObjectNode();

    if (id != null) {
      wrapperNode.set("documentId", TextNode.valueOf(id));
    }
    if (node != null) {
      wrapperNode.set("data", node);
    }
    wrapperNode.set("pageState", TextNode.valueOf(pagingState));
    return wrapperNode;
  }

  @POST
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection}")
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
          boolean success =
              schemalessService.putAtRoot(
                  authToken, keyspace, collection, newId, payload, dbFactory);

          if (success) {
            return Response.created(
                    URI.create(
                        String.format(
                            "/v2/namespaces/%s/collections/%s/%s", keyspace, collection, newId)))
                .entity(mapper.writeValueAsString(wrapResponse(null, newId)))
                .build();
          } else {
            // This should really never happen, just being defensive
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(String.format("Fatal ID collision, try once more: %s", newId))
                .build();
          }
        });
  }

  @PUT
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection}/{id}")
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
          boolean success =
              schemalessService.putAtRoot(authToken, keyspace, collection, id, payload, dbFactory);

          if (success) {
            return Response.ok().entity(mapper.writeValueAsString(wrapResponse(null, id))).build();
          } else {
            return Response.status(Response.Status.CONFLICT)
                .entity(
                    String.format("Document %s already exists in collection %s", id, collection))
                .build();
          }
        });
  }

  @PUT
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection}/{id}/{path: .*}")
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
          return Response.ok().entity(mapper.writeValueAsString(wrapResponse(null, id))).build();
        });
  }

  @PATCH
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection}/{id}")
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
          return Response.ok().entity(mapper.writeValueAsString(wrapResponse(null, id))).build();
        });
  }

  @PATCH
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection}/{id}/{path: .*}")
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
          return Response.ok().entity(mapper.writeValueAsString(wrapResponse(null, id))).build();
        });
  }

  @DELETE
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection: [a-zA-Z_0-9]+}/{id}")
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
  @Path(
      "namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
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
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection: [a-zA-Z_0-9]+}/{id}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response getDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size") int pageSizeParam,
      @QueryParam("page-state") String pageStateParam,
      @QueryParam("raw") Boolean raw) {
    return getDocPath(
        headers,
        ui,
        authToken,
        keyspace,
        collection,
        id,
        new ArrayList<>(),
        where,
        fields,
        pageSizeParam,
        pageStateParam,
        raw);
  }

  @GET
  @Path(
      "namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection: [a-zA-Z_0-9]+}/{id}/{path: .*}")
  @Consumes("application/json")
  @Produces("application/json")
  public Response getDocPath(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @PathParam("id") String id,
      @PathParam("path") List<PathSegment> path,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size") int pageSizeParam,
      @QueryParam("page-state") String pageStateParam,
      @QueryParam("raw") Boolean raw) {
    return handle(
        () -> {
          List<FilterCondition> filters = new ArrayList<>();
          List<String> selectionList = new ArrayList<>();
          if (where != null) {
            JsonNode filterJson = mapper.readTree(where);
            filters = schemalessService.convertToFilterOps(path, filterJson);
            if (fields != null) {
              JsonNode fieldsJson = mapper.readTree(fields);
              selectionList = schemalessService.convertToSelectionList(fieldsJson);
            }
          } else if (fields != null) {
            throw new SchemalessRequestException("Selecting fields is not allowed without `where`");
          }

          if (!filters.isEmpty()) {
            Set<String> distinctFields =
                filters.stream().map(FilterCondition::getFullFieldPath).collect(Collectors.toSet());
            if (distinctFields.size() > 1) {
              throw new SchemalessRequestException(
                  String.format(
                      "Conditions across multiple fields are not yet supported (found: %s)",
                      distinctFields));
            }
            String fieldName = filters.get(0).getField();
            if (!selectionList.isEmpty() && !selectionList.contains(fieldName)) {
              throw new SchemalessRequestException(
                  "When selecting `fields`, the field referenced by `where` must be in the selection.");
            }
          }

          JsonNode node;
          if (filters.isEmpty()) {
            DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);
            node = schemalessService.getJsonAtPath(db, keyspace, collection, id, path);
            if (node == null) {
              return Response.noContent().build();
            }

            if (raw == null || !raw) {
              node = wrapResponse(node, id);
            }

            String json = mapper.writeValueAsString(node);

            logger.debug(json);
            return Response.ok(json).build();
          } else {
            ByteBuffer pageState = null;
            if (pageStateParam != null) {
              byte[] decodedBytes = Base64.getDecoder().decode(pageStateParam);
              pageState = ByteBuffer.wrap(decodedBytes);
            }
            DocumentDB db =
                dbFactory.getDocDataStoreForToken(
                    authToken, pageSizeParam > 0 ? pageSizeParam : DEFAULT_PAGE_SIZE, pageState);
            ImmutablePair<JsonNode, ByteBuffer> result =
                schemalessService.searchDocumentsV2(
                    db, keyspace, collection, filters, selectionList, id);

            if (result == null) {
              return Response.noContent().build();
            }

            if (raw == null || !raw) {
              String pagingStateStr =
                  result.right != null
                      ? Base64.getEncoder().encodeToString(result.right.array())
                      : null;
              node = wrapResponse(result.left, id, pagingStateStr);
            } else {
              node = result.left;
            }

            String json = mapper.writeValueAsString(node);

            logger.debug(json);
            return Response.ok(json).build();
          }
        });
  }

  @GET
  @Path("namespaces/{keyspace: [a-zA-Z_0-9]+}/collections/{collection: [a-zA-Z_0-9]+}")
  @Produces("application/json")
  public Response searchDoc(
      @Context HttpHeaders headers,
      @Context UriInfo ui,
      @HeaderParam("X-Cassandra-Token") String authToken,
      @PathParam("keyspace") String keyspace,
      @PathParam("collection") String collection,
      @QueryParam("where") String where,
      @QueryParam("fields") String fields,
      @QueryParam("page-size") int pageSizeParam,
      @QueryParam("page-state") String pageStateParam,
      // TODO: Someday, support this in a non-restrictive way
      // @QueryParam("sort") String sort,
      @QueryParam("raw") Boolean raw) {
    return handle(
        () -> {
          List<FilterCondition> filters = new ArrayList<>();
          List<String> selectionList = new ArrayList<>();
          if (where != null) {
            JsonNode filterJson = mapper.readTree(where);
            filters = schemalessService.convertToFilterOps(new ArrayList<>(), filterJson);
          }

          if (fields != null) {
            JsonNode fieldsJson = mapper.readTree(fields);
            selectionList = schemalessService.convertToSelectionList(fieldsJson);
          }

          if (!filters.isEmpty()) {
            Set<String> distinctFields =
                filters.stream().map(FilterCondition::getFullFieldPath).collect(Collectors.toSet());
            if (distinctFields.size() > 1) {
              throw new SchemalessRequestException(
                  String.format(
                      "Conditions across multiple fields are not yet supported (found: %s)",
                      distinctFields));
            }
          }

          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            byte[] decodedBytes = Base64.getDecoder().decode(pageStateParam);
            pageState = ByteBuffer.wrap(decodedBytes);
          }

          int pageSize = DEFAULT_PAGE_SIZE;

          ByteBuffer cloneState = pageState != null ? pageState.duplicate() : null;
          DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, pageSize, pageState);

          ImmutablePair<JsonNode, ByteBuffer> results;

          if (pageSizeParam > 20) {
            throw new SchemalessRequestException("The parameter `page-size` is limited to 20.");
          }
          if (filters.isEmpty()) {
            results =
                schemalessService.getFullDocuments(
                    dbFactory,
                    db,
                    authToken,
                    keyspace,
                    collection,
                    selectionList,
                    cloneState,
                    pageSize,
                    Math.max(1, pageSizeParam));
          } else {
            results =
                schemalessService.getFullDocumentsFiltered(
                    dbFactory,
                    db,
                    authToken,
                    keyspace,
                    collection,
                    filters,
                    selectionList,
                    cloneState,
                    pageSize,
                    Math.max(1, pageSizeParam));
          }

          if (results == null) {
            return Response.noContent().build();
          }

          JsonNode docsResult = results.left;
          String pagingStateStr =
              results.right != null
                  ? Base64.getEncoder().encodeToString(results.right.array())
                  : null;

          if (raw == null || !raw) {
            docsResult = wrapResponse(docsResult, null, pagingStateStr);
          }

          String json = mapper.writeValueAsString(docsResult);

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
