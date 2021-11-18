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

import static io.stargate.proto.QueryOuterClass.TypeSpec.Udt;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema.UserDefinedTypeCreate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

class CreateUserDefinedType extends DdlMessageHandler<UserDefinedTypeCreate> {

  CreateUserDefinedType(
      UserDefinedTypeCreate createUdt,
      Persistence.Connection connection,
      Persistence persistence,
      TypedValue.Codec queryBuilder,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(
        createUdt,
        connection,
        persistence,
        queryBuilder,
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
      UserDefinedTypeCreate createUdtProto, Persistence persistence, QueryBuilder queryBuilder) {
    String keyspaceName =
        persistence.decorateKeyspaceName(
            createUdtProto.getKeyspaceName(), GrpcService.HEADERS_KEY.get());
    Udt udtProto = createUdtProto.getUdt();

    ImmutableUserDefinedType udt =
        ImmutableUserDefinedType.builder()
            .keyspace(keyspaceName)
            .name(udtProto.getName())
            .columns(
                udtProto.getFieldsMap().entrySet().stream()
                    .map(
                        entry ->
                            ImmutableColumn.create(
                                entry.getKey(), Column.Kind.Regular, convertType(entry.getValue())))
                    .collect(Collectors.toList()))
            .build();

    return queryBuilder
        .create()
        .type(keyspaceName, udt)
        .ifNotExists(createUdtProto.getIfNotExists())
        .build()
        .bind()
        .queryString();
  }
}
