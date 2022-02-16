package io.stargate.sgv2.docssvc.resources;

import io.stargate.proto.StargateBridgeGrpc;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

// note: JAX-RS Class Annotations MUST be in the impl class; only method annotations inherited
// (but Swagger allows inheritance)
@Path("/v2/namespaces/{namespace-id: [a-zA-Z_0-9]+}/collections/{collection-id: [a-zA-Z_0-9]+}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2SearchDocsResourceImpl extends ResourceBase implements Sgv2SearchDocsResourceApi {
  /*
  /////////////////////////////////////////////////////////////////////////
  // Docs API search endpoint implementation
  /////////////////////////////////////////////////////////////////////////
   */

  @Override
  public Response searchCollection(
      final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      final String namespace,
      final String collection,
      final String where,
      final String fields,
      final int pageSizeParam,
      final String pageStateParam,
      final boolean raw,
      final HttpServletRequest request) {
    //    requireNonEmptyKeyspaceAndTable(keyspaceName, tableName);

    return Response.ok("pong").build();
  }
}
