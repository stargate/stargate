package io.stargate.sgv2.docssvc.resources;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Value;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.docssvc.grpc.ToProtoConverter;
import io.stargate.sgv2.docssvc.models.Sgv2RESTResponse;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  public Response searchDocs(
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
