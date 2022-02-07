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
package io.stargate.bridge.service;

import com.google.protobuf.StringValue;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.SchemaChange.Target;
import io.stargate.proto.QueryOuterClass.SchemaChange.Type;
import io.stargate.proto.Schema.SchemaNotification;
import io.stargate.proto.Schema.SchemaNotification.Ready;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaNotificationsHandler implements EventListener {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaNotificationsHandler.class);

  private final Persistence persistence;
  private final ServerCallStreamObserver<SchemaNotification> responseObserver;
  private final AtomicBoolean ready = new AtomicBoolean();
  private boolean isFailed;

  SchemaNotificationsHandler(
      Persistence persistence, StreamObserver<SchemaNotification> responseObserver) {
    this.persistence = persistence;
    this.responseObserver = (ServerCallStreamObserver<SchemaNotification>) responseObserver;
  }

  public void handle() {
    synchronized (responseObserver) {
      responseObserver.setOnCancelHandler(() -> persistence.unregisterEventListener(this));
      responseObserver.setOnCloseHandler(() -> persistence.unregisterEventListener(this));
    }
    persistence.registerEventListener(this);
    markReady();
  }

  private void onSchemaChange(
      Type type,
      Target target,
      String keyspaceName,
      String elementName,
      List<String> argumentTypes) {
    synchronized (responseObserver) {
      if (isFailed) {
        return;
      }
      try {
        markReady();

        SchemaChange.Builder change =
            SchemaChange.newBuilder()
                .setChangeType(type)
                .setTarget(target)
                .setKeyspace(keyspaceName);
        if (elementName != null) {
          change.setName(StringValue.of(elementName));
        }
        change.addAllArgumentTypes(argumentTypes);

        responseObserver.onNext(SchemaNotification.newBuilder().setChange(change).build());
      } catch (Throwable t) {
        try {
          // responseObserver.onCloseHandler runs asynchronously, so more events can arrive before
          // we've had a chance to unregister. Make sure we won't notify the observer anymore.
          isFailed = true;

          responseObserver.onError(t);
        } catch (Throwable t2) {
          // Be defensive here because the Cassandra internals don't guard against listener errors.
          t2.addSuppressed(t);
          LOG.warn("Unexpected error while notifying error", t2);
        }
      }
    }
  }

  // Send the Ready message right after we get control back from registering the observer, or right
  // before the first real notification, whichever happens first.
  private void markReady() {
    if (ready.compareAndSet(false, true)) {
      synchronized (responseObserver) {
        responseObserver.onNext(
            SchemaNotification.newBuilder().setReady(Ready.newBuilder().build()).build());
      }
    }
  }

  @Override
  public void onCreateKeyspace(String keyspace) {
    onSchemaChange(Type.CREATED, Target.KEYSPACE, keyspace, null, Collections.emptyList());
  }

  @Override
  public void onCreateTable(String keyspace, String table) {
    onSchemaChange(Type.CREATED, Target.TABLE, keyspace, table, Collections.emptyList());
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    onSchemaChange(Type.CREATED, Target.TABLE, keyspace, view, Collections.emptyList());
  }

  @Override
  public void onCreateType(String keyspace, String type) {
    onSchemaChange(Type.CREATED, Target.TYPE, keyspace, type, Collections.emptyList());
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
    onSchemaChange(Type.UPDATED, Target.KEYSPACE, keyspace, null, Collections.emptyList());
  }

  @Override
  public void onAlterTable(String keyspace, String table) {
    onSchemaChange(Type.UPDATED, Target.TABLE, keyspace, table, Collections.emptyList());
  }

  @Override
  public void onAlterView(String keyspace, String view) {
    onSchemaChange(Type.UPDATED, Target.TABLE, keyspace, view, Collections.emptyList());
  }

  @Override
  public void onAlterType(String keyspace, String type) {
    onSchemaChange(Type.UPDATED, Target.TYPE, keyspace, type, Collections.emptyList());
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
    onSchemaChange(Type.DROPPED, Target.KEYSPACE, keyspace, null, Collections.emptyList());
  }

  @Override
  public void onDropTable(String keyspace, String table) {
    onSchemaChange(Type.DROPPED, Target.TABLE, keyspace, table, Collections.emptyList());
  }

  @Override
  public void onDropView(String keyspace, String view) {
    onSchemaChange(Type.DROPPED, Target.TABLE, keyspace, view, Collections.emptyList());
  }

  @Override
  public void onDropType(String keyspace, String type) {
    onSchemaChange(Type.DROPPED, Target.TYPE, keyspace, type, Collections.emptyList());
  }

  @Override
  public void onDropFunction(String keyspace, String function, List<String> argumentTypes) {
    onSchemaChange(Type.DROPPED, Target.FUNCTION, keyspace, function, argumentTypes);
  }

  @Override
  public void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onSchemaChange(Type.DROPPED, Target.AGGREGATE, keyspace, aggregate, argumentTypes);
  }
}
