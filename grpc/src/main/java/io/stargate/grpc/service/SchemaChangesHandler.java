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

import com.google.protobuf.StringValue;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.SchemaChange.Target;
import io.stargate.proto.QueryOuterClass.SchemaChange.Type;
import java.util.List;

class SchemaChangesHandler implements EventListener {

  private final Persistence persistence;
  private final ServerCallStreamObserver<QueryOuterClass.SchemaChange> responseObserver;

  SchemaChangesHandler(
      Persistence persistence, StreamObserver<QueryOuterClass.SchemaChange> responseObserver) {
    this.persistence = persistence;
    this.responseObserver =
        (ServerCallStreamObserver<QueryOuterClass.SchemaChange>) responseObserver;
  }

  public void handle() {
    persistence.registerEventListener(this);
    responseObserver.setOnCancelHandler(
        () -> {
          persistence.unregisterEventListener(this);
        });
  }

  @Override
  public void onCreateKeyspace(String keyspace) {
    onKeyspaceChange(Type.CREATED, keyspace);
  }

  @Override
  public void onCreateTable(String keyspace, String table) {
    onSchemaChange(Type.CREATED, Target.TABLE, keyspace, table);
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    onSchemaChange(Type.CREATED, Target.TABLE, keyspace, view);
  }

  @Override
  public void onCreateType(String keyspace, String type) {
    onSchemaChange(Type.CREATED, Target.TYPE, keyspace, type);
  }

  @Override
  public void onCreateFunction(String keyspace, String function, List<String> argumentTypes) {
    onSchemaChange(Type.CREATED, Target.FUNCTION, keyspace, function, argumentTypes);
  }

  @Override
  public void onCreateAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onSchemaChange(Type.CREATED, Target.AGGREGATE, keyspace, aggregate, argumentTypes);
  }

  @Override
  public void onAlterKeyspace(String keyspace) {
    onKeyspaceChange(Type.UPDATED, keyspace);
  }

  @Override
  public void onAlterTable(String keyspace, String table) {
    onSchemaChange(Type.UPDATED, Target.TABLE, keyspace, table);
  }

  @Override
  public void onAlterView(String keyspace, String view) {
    onSchemaChange(Type.UPDATED, Target.TABLE, keyspace, view);
  }

  @Override
  public void onAlterType(String keyspace, String type) {
    onSchemaChange(Type.UPDATED, Target.TYPE, keyspace, type);
  }

  @Override
  public void onAlterFunction(String keyspace, String function, List<String> argumentTypes) {
    onSchemaChange(Type.UPDATED, Target.FUNCTION, keyspace, function, argumentTypes);
  }

  @Override
  public void onAlterAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onSchemaChange(Type.CREATED, Target.AGGREGATE, keyspace, aggregate, argumentTypes);
  }

  @Override
  public void onDropKeyspace(String keyspace) {
    onKeyspaceChange(Type.DROPPED, keyspace);
  }

  @Override
  public void onDropTable(String keyspace, String table) {
    onSchemaChange(Type.DROPPED, Target.TABLE, keyspace, table);
  }

  @Override
  public void onDropView(String keyspace, String view) {
    onSchemaChange(Type.DROPPED, Target.TABLE, keyspace, view);
  }

  @Override
  public void onDropType(String keyspace, String type) {
    onSchemaChange(Type.DROPPED, Target.TYPE, keyspace, type);
  }

  @Override
  public void onDropFunction(String keyspace, String function, List<String> argumentTypes) {
    onSchemaChange(Type.DROPPED, Target.FUNCTION, keyspace, function, argumentTypes);
  }

  @Override
  public void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onSchemaChange(Type.DROPPED, Target.AGGREGATE, keyspace, aggregate, argumentTypes);
  }

  private void onKeyspaceChange(Type type, String keyspace) {
    responseObserver.onNext(
        QueryOuterClass.SchemaChange.newBuilder()
            .setChangeType(type)
            .setTarget(Target.KEYSPACE)
            .setKeyspace(keyspace)
            .build());
  }

  private void onSchemaChange(Type type, Target target, String keyspace, String name) {
    responseObserver.onNext(newBuilder(type, target, keyspace, name).build());
  }

  private void onSchemaChange(
      Type type, Target target, String keyspace, String name, List<String> argumentTypes) {
    responseObserver.onNext(
        newBuilder(type, target, keyspace, name).addAllArgumentTypes(argumentTypes).build());
  }

  private QueryOuterClass.SchemaChange.Builder newBuilder(
      Type type, Target target, String keyspace, String name) {
    return QueryOuterClass.SchemaChange.newBuilder()
        .setChangeType(type)
        .setTarget(target)
        .setKeyspace(keyspace)
        .setName(StringValue.of(name));
  }
}
