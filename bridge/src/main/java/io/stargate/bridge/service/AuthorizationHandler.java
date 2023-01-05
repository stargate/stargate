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

import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.bridge.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.bridge.proto.Schema.AuthorizeSchemaReadsResponse;
import io.stargate.bridge.proto.Schema.SchemaRead;
import io.stargate.db.Persistence.Connection;
import java.util.Collections;

class AuthorizationHandler {

  private final AuthorizeSchemaReadsRequest request;
  private final Connection connection;
  private final SourceAPI sourceAPI;
  private final AuthorizationService authorizationService;
  private final StreamObserver<AuthorizeSchemaReadsResponse> responseObserver;

  AuthorizationHandler(
      AuthorizeSchemaReadsRequest request,
      Connection connection,
      SourceAPI sourceAPI,
      AuthorizationService authorizationService,
      StreamObserver<AuthorizeSchemaReadsResponse> responseObserver) {
    this.request = request;
    this.connection = connection;
    this.sourceAPI = sourceAPI;
    this.authorizationService = authorizationService;
    this.responseObserver = responseObserver;
  }

  void handle() {
    AuthorizeSchemaReadsResponse.Builder response = AuthorizeSchemaReadsResponse.newBuilder();
    AuthenticationSubject subject = getSubject();
    request
        .getSchemaReadsList()
        .forEach(read -> response.addAuthorized(isAuthorized(subject, read)));
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  private boolean isAuthorized(AuthenticationSubject subject, SchemaRead read) {
    try {
      authorizationService.authorizeSchemaRead(
          subject,
          Collections.singletonList(read.getKeyspaceName()),
          read.hasElementName()
              ? Collections.singletonList(read.getElementName().getValue())
              : Collections.emptyList(),
          sourceAPI,
          convertType(read.getElementType()));
      return true;
    } catch (UnauthorizedException e) {
      return false;
    }
  }

  private AuthenticationSubject getSubject() {
    return connection
        .loggedUser()
        .map(AuthenticationSubject::of)
        .orElseThrow(() -> new IllegalStateException("Must be authenticated"));
  }

  private ResourceKind convertType(SchemaRead.ElementType elementType) {
    switch (elementType) {
      case KEYSPACE:
        return ResourceKind.KEYSPACE;
      case TABLE:
        return ResourceKind.TABLE;
      case FUNCTION:
        return ResourceKind.FUNCTION;
      case TYPE:
        return ResourceKind.TYPE;
      case TRIGGER:
        return ResourceKind.TRIGGER;
      case AGGREGATE:
        return ResourceKind.AGGREGATE;
      case VIEW:
        return ResourceKind.VIEW;
      case INDEX:
        return ResourceKind.INDEX;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unsupported element type " + elementType);
    }
  }
}
