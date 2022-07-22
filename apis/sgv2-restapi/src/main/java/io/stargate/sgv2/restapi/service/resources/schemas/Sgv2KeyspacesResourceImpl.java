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
package io.stargate.sgv2.restapi.service.resources.schemas;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.restapi.service.models.Sgv2Keyspace;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sgv2KeyspacesResourceImpl extends RestResourceBase
    implements Sgv2KeyspacesResourceApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(Sgv2KeyspacesResourceImpl.class);

  private static final JsonMapper JSON_MAPPER = new JsonMapper();
  private static final ObjectReader REPLICA_SETTINGS_READER =
      JSON_MAPPER.readerFor(JsonNode.class).with(JsonReadFeature.ALLOW_SINGLE_QUOTES);

  private static final SchemaBuilderHelper schemaBuilder = new SchemaBuilderHelper(JSON_MAPPER);

  @Override
  public Response getAllKeyspaces(final boolean raw) {

    List<Sgv2Keyspace> keyspaces =
        bridge.getAllKeyspaces().stream()
            .map(Sgv2KeyspacesResourceImpl::keyspaceFrom)
            .collect(Collectors.toList());

    final Object payload = raw ? keyspaces : new Sgv2RESTResponse<>(keyspaces);
    return Response.status(Status.OK).entity(payload).build();
  }

  @Override
  public Response getOneKeyspace(final String keyspaceName, final boolean raw) {
    return bridge
        .getKeyspace(keyspaceName, true)
        .map(
            describe -> {
              Sgv2Keyspace keyspace = keyspaceFrom(describe);

              final Object payload = raw ? keyspace : new Sgv2RESTResponse<>(keyspace);
              return Response.status(Status.OK).entity(payload).build();
            })
        .orElseThrow(
            () -> new WebApplicationException("unable to describe keyspace", Status.NOT_FOUND));
  }

  @Override
  public Response createKeyspace(final JsonNode payload) {
    SchemaBuilderHelper.KeyspaceCreateDefinition ksCreateDef;
    try {
      ksCreateDef = schemaBuilder.readKeyspaceCreateDefinition(payload);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }
    final String keyspaceName = ksCreateDef.name;
    Query query;
    if (ksCreateDef.datacenters == null) {
      query =
          new QueryBuilder()
              .create()
              .keyspace(keyspaceName)
              .ifNotExists()
              .withReplication(Replication.simpleStrategy(ksCreateDef.replicas))
              .build();
    } else {
      Map<String, Integer> dcMap = ksCreateDef.datacentersAsMap();
      query =
          new QueryBuilder()
              .create()
              .keyspace(keyspaceName)
              .ifNotExists()
              .withReplication(Replication.networkTopologyStrategy(dcMap))
              .build();
    }

    bridge.executeQuery(query);

    // No real contents; can ignore ResultSet it seems and only worry about exceptions

    final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspaceName);
    return Response.status(Status.CREATED).entity(responsePayload).build();
  }

  @Override
  public Response deleteKeyspace(final String keyspaceName) {
    Query query = new QueryBuilder().drop().keyspace(keyspaceName).ifExists().build();
    /*QueryOuterClass.Response grpcResponse =*/ bridge.executeQuery(query);
    return Response.status(Status.NO_CONTENT).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for structural conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private static Sgv2Keyspace keyspaceFrom(CqlKeyspaceDescribe describe) {
    Schema.CqlKeyspace keyspace = describe.getCqlKeyspace();
    Sgv2Keyspace ks = new Sgv2Keyspace(keyspace.getName());

    Map<String, String> options = keyspace.getOptionsMap();
    String replication = options.get("replication");
    if (replication != null && !replication.isEmpty()) {
      try {
        JsonNode replicaSettings = REPLICA_SETTINGS_READER.readValue(replication);
        // Ugh, this gets ugly; "options" has "class" for strategy, then DC:replica-count
        // as entries. Also, cannot remove from Map.
        JsonNode strategyNode = replicaSettings.path("class");
        if ("NetworkTopologyStrategy".equals(strategyNode.asText())) {
          Iterator<Map.Entry<String, JsonNode>> it = replicaSettings.fields();
          while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            JsonNode value = entry.getValue();
            if (value.isIntegralNumber()) {
              ks.addDatacenter(entry.getKey(), value.asInt());
            }
          }
        }
      } catch (IOException e) {
        LOGGER.warn(
            "Malformed 'replication' settings for keyspace {} (problem: {}), input: {}",
            keyspace.getName(),
            e.getMessage(),
            replication);
      }
    }

    return ks;
  }
}
