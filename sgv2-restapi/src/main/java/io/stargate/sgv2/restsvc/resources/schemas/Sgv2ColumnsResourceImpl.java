package io.stargate.sgv2.restsvc.resources.schemas;

import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2ColumnsResourceImpl extends ResourceBase implements Sgv2ColumnsResourceApi {
  @Override
  public Response getAllColumns(
      String token,
      String keyspaceName,
      String tableName,
      boolean raw,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response createColumn(
      String token,
      String keyspaceName,
      String tableName,
      Sgv2ColumnDefinition columnDefinition,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response getOneColumn(
      String token,
      String keyspaceName,
      String tableName,
      String columnName,
      boolean raw,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response updateColumn(
      String token,
      String keyspaceName,
      String tableName,
      String columnName,
      Sgv2ColumnDefinition columnUpdate,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }

  @Override
  public Response deleteColumn(
      String token,
      String keyspaceName,
      String tableName,
      String columnName,
      HttpServletRequest request) {
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
  }
}
