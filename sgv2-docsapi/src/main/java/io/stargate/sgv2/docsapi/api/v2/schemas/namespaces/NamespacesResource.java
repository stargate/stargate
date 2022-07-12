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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.v2.schemas.namespaces;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.SimpleResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.model.dto.Datacenter;
import io.stargate.sgv2.docsapi.api.v2.schemas.namespaces.model.dto.NamespaceDto;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import java.net.URI;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(NamespacesResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = OpenApiConstants.Tags.NAMESPACES)
public class NamespacesResource {

  private static final Logger LOG = LoggerFactory.getLogger(NamespacesResource.class);

  public static final String BASE_PATH = "/v2/schemas/namespaces";

  @Inject NamespaceManager namespaceManager;

  @Inject ObjectMapper objectMapper;

  @Operation(
      summary = "List namespaces",
      description =
          "List all available namespaces. Note that a namespace is an equivalent to the Cassandra keyspace.")
  @Parameters(
      value = {
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = SimpleResponseWrapper.class,
                          properties =
                              @SchemaProperty(
                                  name = "data",
                                  type = SchemaType.ARRAY,
                                  implementation = NamespaceDto.class,
                                  uniqueItems = true,
                                  minItems = 0)))
            }),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @GET
  public Uni<RestResponse<Object>> getAllNamespaces(@QueryParam("raw") boolean raw) {

    // get all
    return namespaceManager
        .getNamespaces()

        // then map and collect
        .map(this::toNamespaceDto)
        .collect()
        .asList()

        // map to wrapper if needed
        .map(
            results -> {
              if (raw) {
                return results;
              } else {
                return new SimpleResponseWrapper<>(results);
              }
            })
        .map(RestResponse::ok);
  }

  @Operation(
      summary = "Get a namespace",
      description =
          "Retrieve a single namespace specification. Note that a namespace is an equivalent to the Cassandra keyspace.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace name."),
        @Parameter(name = "raw", ref = OpenApiConstants.Parameters.RAW),
      })
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "200",
            description =
                "Call successful. Note that in case of unwrapping (`raw=true`), the response contains only the contents of the `data` property.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = SimpleResponseWrapper.class,
                          properties =
                              @SchemaProperty(name = "data", implementation = NamespaceDto.class)))
            }),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST)
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @GET
  @Path("/{namespace:\\w+}")
  public Uni<RestResponse<Object>> getNamespace(
      @PathParam("namespace") String namespace, @QueryParam("raw") boolean raw) {

    // get all
    return namespaceManager
        .getNamespace(namespace)

        // then map and collect
        .map(this::toNamespaceDto)

        // map to wrapper if needed
        .map(
            result -> {
              if (raw) {
                return result;
              } else {
                return new SimpleResponseWrapper<>(result);
              }
            })
        .map(RestResponse::ok);
  }

  @Operation(
      summary = "Create a namespace",
      description =
          "Create a new namespace using given replication strategy. Note that a namespace is an equivalent to the Cassandra keyspace.")
  @RequestBody(
      description = "Request body",
      content =
          @Content(
              examples = {
                @ExampleObject(
                    name = "With SimpleStrategy",
                    value =
                        """
                        {
                            "name": "cycling",
                            "replicas": 1
                        }
                        """),
                @ExampleObject(
                    name = "With NetworkTopologyStrategy",
                    value =
                        """
                        {
                            "name": "cycling",
                            "datacenters": [
                                { "name": "dc1", "replicas": "3" },
                                { "name": "dc2", "replicas": "3" }
                            ]
                        }
                        """),
              }))
  @APIResponses(
      value = {
        @APIResponse(
            responseCode = "201",
            description = "Namespace created.",
            content = {
              @Content(
                  schema =
                      @org.eclipse.microprofile.openapi.annotations.media.Schema(
                          implementation = NamespaceDto.class))
            }),
        @APIResponse(
            responseCode = "409",
            description = "Conflict.",
            content =
                @Content(
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class,
                            example =
                                """
                                {
                                    "code": 409,
                                    "description": "Create failed: namespace cycling already exists."
                                }
                                """))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_400),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @POST
  public Uni<RestResponse<Object>> createNamespace(
      @Context UriInfo uriInfo,
      @NotNull(message = "payload not provided") @Valid NamespaceDto body) {
    String namespaceName = body.name();

    // check existing
    return namespaceManager
        .getNamespace(namespaceName)

        // in case there is a namespace, ensure we report as error
        .map(
            namespace -> {
              int code = Response.Status.CONFLICT.getStatusCode();
              ApiError error =
                  new ApiError(
                      "Create failed: namespace %s already exists.".formatted(namespaceName), code);
              return RestResponse.ResponseBuilder.create(code).entity(error).build();
            })

        // otherwise if namespace does not exist, manager would fire the error
        // with DATASTORE_KEYSPACE_DOES_NOT_EXIST
        .onFailure(
            t -> {
              if (t instanceof ErrorCodeRuntimeException ec) {
                return Objects.equals(
                    ec.getErrorCode(), ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST);
              }
              return false;
            })
        .recoverWithUni(
            () ->
                namespaceManager
                    .createNamespace(namespaceName, getReplication(body))
                    .map(
                        created -> {
                          URI location =
                              uriInfo
                                  .getBaseUriBuilder()
                                  .path(BASE_PATH)
                                  .path(namespaceName)
                                  .build();
                          NamespaceDto entity =
                              new NamespaceDto(namespaceName, body.replicas(), body.datacenters());
                          return RestResponse.ResponseBuilder.created(location)
                              .entity(entity)
                              .build();
                        }));
  }

  @Operation(
      summary = "Delete a namespace",
      description =
          "Delete a namespace if exists. Note that a namespace is an equivalent to the Cassandra keyspace.")
  @Parameters(
      value = {
        @Parameter(
            name = "namespace",
            ref = OpenApiConstants.Parameters.NAMESPACE,
            description = "The namespace name."),
      })
  @APIResponses(
      value = {
        @APIResponse(responseCode = "204", description = "Namespace deleted."),
        @APIResponse(
            responseCode = "404",
            description = "Not found.",
            content =
                @Content(
                    examples = {
                      @ExampleObject(ref = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST),
                    },
                    schema =
                        @org.eclipse.microprofile.openapi.annotations.media.Schema(
                            implementation = ApiError.class))),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_401),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_500),
        @APIResponse(ref = OpenApiConstants.Responses.GENERAL_503),
      })
  @DELETE
  @Path("/{namespace:\\w+}")
  public Uni<RestResponse<Object>> deleteNamespace(@PathParam("namespace") String namespace) {
    // go delete the keyspace
    return namespaceManager.dropNamespace(namespace).map(any -> RestResponse.noContent());
  }

  // mapper from CqlKeyspaceDescribe to NamespaceDto
  private NamespaceDto toNamespaceDto(Schema.CqlKeyspaceDescribe keyspaceDescribe) {
    Schema.CqlKeyspace keyspace = keyspaceDescribe.getCqlKeyspace();
    String name = keyspace.getName();
    List<Datacenter> datacenters = buildDatacenters(keyspace);
    Integer replicas = getSimpleStrategyReplicas(keyspace).orElse(null);
    return new NamespaceDto(name, replicas, datacenters);
  }

  private Optional<Integer> getSimpleStrategyReplicas(Schema.CqlKeyspace keyspace) {
    Map<String, String> options = keyspace.getOptionsMap();
    String replication = options.get("replication");

    // null if no replication or is not NetworkTopologyStrategy
    if (null == replication || !replication.contains("SimpleStrategy")) {
      return Optional.empty();
    }

    // there's no easy way to do this, so a bit of hacking
    String replicationJson = replication.replace('\'', '"');
    try {
      // read tree
      JsonNode replicationNode = objectMapper.readTree(replicationJson);

      // then try to get replication_factor
      return Optional.ofNullable(replicationNode.get("replication_factor"))
          .filter(JsonNode::isNumber)
          .map(JsonNode::intValue);
    } catch (JsonProcessingException e) {
      LOG.warn("Error parsing the keyspace replication from {}.", replicationJson, e);
      return Optional.empty();
    }
  }

  // figure out data centers
  private List<Datacenter> buildDatacenters(Schema.CqlKeyspace keyspace) {
    Map<String, String> options = keyspace.getOptionsMap();
    String replication = options.get("replication");

    // null if no replication or is not NetworkTopologyStrategy
    if (null == replication || !replication.contains("NetworkTopologyStrategy")) {
      return null;
    }

    // there's no easy way to do this, so a bit of hacking
    String replicationJson = replication.replace('\'', '"');
    try {
      // read tree
      JsonNode replicationNode = objectMapper.readTree(replicationJson);
      Spliterator<Map.Entry<String, JsonNode>> fields =
          Spliterators.spliteratorUnknownSize(replicationNode.fields(), Spliterator.ORDERED);

      // stream fields
      return StreamSupport.stream(fields, false)

          // skip any that is not number and skip class prop
          .filter(f -> !Objects.equals("class", f.getKey()) && f.getValue().isNumber())

          // map to data center and collect
          .map(f -> new Datacenter(f.getKey(), f.getValue().asInt()))

          // sort by name
          .sorted(Comparator.comparing(Datacenter::name))
          .collect(Collectors.toList());

    } catch (JsonProcessingException e) {
      LOG.warn("Error parsing the keyspace replication from {}.", replicationJson, e);
      return null;
    }
  }

  // extracts replication from the CreateNamespaceDto
  public Replication getReplication(NamespaceDto namespaceDto) {
    Collection<Datacenter> datacenters = namespaceDto.datacenters();
    if (null == datacenters || datacenters.isEmpty()) {
      int replicas = Optional.ofNullable(namespaceDto.replicas()).orElse(1);
      return Replication.simpleStrategy(replicas);
    } else {
      Map<String, Integer> datacenterMap =
          datacenters.stream()
              .collect(
                  Collectors.toMap(
                      Datacenter::name, dc -> Optional.ofNullable(dc.replicas()).orElse(3)));
      return Replication.networkTopologyStrategy(datacenterMap);
    }
  }
}
