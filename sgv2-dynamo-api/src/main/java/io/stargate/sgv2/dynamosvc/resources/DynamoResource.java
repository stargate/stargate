package io.stargate.sgv2.dynamosvc.resources;

import static io.stargate.sgv2.dynamosvc.dynamo.Proxy.awsRequestMapper;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.stargate.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.http.CreateStargateBridgeClient;
import io.stargate.sgv2.dynamosvc.dynamo.ItemProxy;
import io.stargate.sgv2.dynamosvc.dynamo.Proxy;
import io.stargate.sgv2.dynamosvc.dynamo.TableProxy;
import io.stargate.sgv2.dynamosvc.models.DynamoStatementType;
import io.swagger.annotations.*;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@ApiImplicitParams({
  @ApiImplicitParam(
      name = "Authorization",
      paramType = "header",
      value = "The token returned from the authorization endpoint. Use this token in each request.",
      required = true)
})
@CreateStargateBridgeClient
public class DynamoResource {

  TableProxy tableProxy;
  ItemProxy itemProxy;

  public DynamoResource(TableProxy tableProxy, ItemProxy itemProxy) {
    this.tableProxy = tableProxy;
    this.itemProxy = itemProxy;
  }

  @Timed
  @POST
  @Consumes("application/x-amz-json-1.0")
  @Produces("application/json")
  public Response handleRequest(
      @Context StargateBridgeClient bridge,
      @Context HttpHeaders headers,
      @HeaderParam("X-Amz-Target") String target,
      String payload) {
    target = target.split("\\.")[1];
    DynamoStatementType statementType = DynamoStatementType.valueOf(target);
    byte[] response;
    try {
      AmazonWebServiceResult result;
      switch (statementType) {
        case CreateTable:
          CreateTableRequest createTableRequest =
              awsRequestMapper.readValue(payload, CreateTableRequest.class);
          result = tableProxy.createTable(createTableRequest, bridge);
          break;
        case PutItem:
          PutItemRequest putItemRequest = awsRequestMapper.readValue(payload, PutItemRequest.class);
          result = itemProxy.putItem(putItemRequest, bridge);
          break;
        case GetItem:
          GetItemRequest getItemRequest = awsRequestMapper.readValue(payload, GetItemRequest.class);
          result = itemProxy.getItem(getItemRequest, bridge);
          break;
        default:
          throw new WebApplicationException(
              "Invalid statement type: " + target, Response.Status.BAD_REQUEST);
      }
      response = awsRequestMapper.writeValueAsBytes(result);
    } catch (JsonProcessingException ex) {
      throw new WebApplicationException("Invalid payload", Response.Status.BAD_REQUEST);
    } catch (IOException ex) {
      throw new WebApplicationException(
          "An error occurred when connecting to Cassandra", Response.Status.INTERNAL_SERVER_ERROR);
    } catch (Exception ex) {
      throw new WebApplicationException(ex.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.status(Response.Status.OK).entity(response).build();
  }

  @Timed
  @GET
  @Produces("application/json")
  @Path("/keyspace/create")
  public Response createKeyspace(
      @Context StargateBridgeClient bridge, @Context HttpHeaders headers) {
    QueryOuterClass.Query query =
        new QueryBuilder()
            .create()
            .keyspace(Proxy.KEYSPACE_NAME)
            .ifNotExists()
            .withReplication(Replication.simpleStrategy(1))
            .build();

    bridge.executeQuery(query);
    final Map<String, Object> responsePayload =
        Collections.singletonMap("name", Proxy.KEYSPACE_NAME);
    return Response.status(Response.Status.CREATED).entity(responsePayload).build();
  }
}
