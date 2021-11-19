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
package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.query.builder.Replication;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceCreate;
import java.util.concurrent.ScheduledExecutorService;

class CreateKeyspaceHandler extends DdlMessageHandler<CqlKeyspaceCreate> {

  CreateKeyspaceHandler(
      CqlKeyspaceCreate createKeyspace,
      Persistence.Connection connection,
      Persistence persistence,
      TypedValue.Codec valueCodec,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(
        createKeyspace,
        connection,
        persistence,
        valueCodec,
        executor,
        schemaAgreementRetries,
        responseObserver);
  }

  @Override
  protected void validate() {
    // intentionally empty
  }

  @Override
  protected String buildQuery(
      CqlKeyspaceCreate createKeyspaceProto, Persistence persistence, QueryBuilder queryBuilder) {
    CqlKeyspace keyspaceProto = createKeyspaceProto.getKeyspace();
    String keyspaceName =
        persistence.decorateKeyspaceName(keyspaceProto.getName(), GrpcService.HEADERS_KEY.get());

    return queryBuilder
        .create()
        .keyspace(keyspaceName)
        .ifNotExists(createKeyspaceProto.getIfNotExists())
        .withReplication(
            keyspaceProto.hasSimpleReplication()
                ? Replication.simpleStrategy(
                    keyspaceProto.getSimpleReplication().getReplicationFactor())
                : Replication.networkTopologyStrategy(
                    keyspaceProto.getNetworkReplication().getReplicationFactorsMap()))
        .andDurableWrites(keyspaceProto.getDurableWrites())
        .build()
        .bind()
        .queryString();
  }
}
