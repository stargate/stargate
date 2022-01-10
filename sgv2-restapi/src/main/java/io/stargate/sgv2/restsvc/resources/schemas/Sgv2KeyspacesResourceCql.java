package io.stargate.sgv2.restsvc.resources.schemas;

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v2/cql/schemas/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2KeyspacesResourceCql {

  private static final SchemaBuilderHelper SCHEMA_BUILDER_HELPER =
      new SchemaBuilderHelper(new JsonMapper());

  @Timed
  @GET
  public Response getAllKeyspaces(
      @Context CqlSession session,
      @QueryParam("raw") final boolean raw,
      @Context HttpServletRequest request) {

    // TODO validate auth token manually here, getMetadata() doesn't require it
    Collection<KeyspaceMetadata> cqlKeyspaces = session.getMetadata().getKeyspaces().values();
    List<Sgv2Keyspace> keyspaces = keyspacesFrom(cqlKeyspaces);

    Object payload = raw ? keyspaces : new Sgv2RESTResponse<>(keyspaces);
    return Response.status(Response.Status.OK).entity(payload).build();
  }

  @Timed
  @GET
  @Path("/{keyspaceName}")
  public Response getOneKeyspace(
      @Context CqlSession session,
      @PathParam("keyspaceName") final String keyspaceName,
      @QueryParam("raw") final boolean raw,
      @Context HttpServletRequest request) {

    // TODO validate auth token manually here, getMetadata() doesn't require it
    KeyspaceMetadata cqlKeyspace =
        session
            .getMetadata()
            .getKeyspace(CqlIdentifier.fromInternal(keyspaceName))
            .orElseThrow(
                () ->
                    new WebApplicationException(
                        "unable to describe keyspace", Response.Status.NOT_FOUND));

    Object payload = raw ? cqlKeyspace : new Sgv2RESTResponse<>(cqlKeyspace);
    return Response.status(Response.Status.OK).entity(payload).build();
  }

  @Timed
  @POST
  public Response createKeyspace(
      @Context CqlSession session, JsonNode payload, @Context HttpServletRequest request) {
    SchemaBuilderHelper.KeyspaceCreateDefinition ksCreateDef;
    try {
      ksCreateDef = SCHEMA_BUILDER_HELPER.readKeyspaceCreateDefinition(payload);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
    }
    CreateKeyspaceStart createStart =
        SchemaBuilder.createKeyspace(CqlIdentifier.fromInternal(ksCreateDef.name)).ifNotExists();
    CreateKeyspace create;
    if (ksCreateDef.datacenters == null) {
      create = createStart.withSimpleStrategy(ksCreateDef.replicas);
    } else {
      create = createStart.withNetworkTopologyStrategy(ksCreateDef.datacenters);
    }
    session.execute(create.asCql());
    final Map<String, Object> responsePayload = Collections.singletonMap("name", ksCreateDef.name);
    return Response.status(Response.Status.CREATED).entity(responsePayload).build();
  }

  @Timed
  @DELETE
  @Path("/{keyspaceName}")
  public Response deleteKeyspace(
      @Context CqlSession session,
      @PathParam("keyspaceName") final String keyspaceName,
      @Context HttpServletRequest request) {
    session.execute(
        SchemaBuilder.dropKeyspace(CqlIdentifier.fromInternal(keyspaceName)).ifExists().asCql());
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  private static List<Sgv2Keyspace> keyspacesFrom(Collection<KeyspaceMetadata> cqlKeyspaces) {
    return cqlKeyspaces.stream()
        .map(Sgv2KeyspacesResourceCql::keyspaceFrom)
        .collect(Collectors.toList());
  }

  private static Sgv2Keyspace keyspaceFrom(KeyspaceMetadata cqlKeyspace) {
    Sgv2Keyspace keyspace = new Sgv2Keyspace(cqlKeyspace.getName().asInternal());
    for (Map.Entry<String, String> entry : cqlKeyspace.getReplication().entrySet()) {
      String dcName = entry.getKey();
      // Datacenters are exposed as Map/Object entries from key to replica count,
      // plus at least one meta-entry ("class") for replication strategy
      if ("class".equals(dcName)) {
        continue;
      }
      keyspace.addDatacenter(dcName, Integer.parseInt(entry.getValue()));
    }
    return keyspace;
  }
}
