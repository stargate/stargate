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
import io.stargate.bridge.service.docsapi.QueryExecutor;
import io.stargate.db.Persistence.Connection;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;

class DocsCollectionSearchHandler {

  private final Schema.QueryDocumentParams request;
  private final Connection connection;
  private final BridgeService bridgeService;
  private final QueryExecutor docsQueryExecutor;
  private final StreamObserver<Schema.DocumentsResponse> responseObserver;

  DocsCollectionSearchHandler(
      Schema.QueryDocumentParams request,
      Connection connection,
      QueryExecutor docsQueryExecutor,
      BridgeService bridgeService,
      StreamObserver<Schema.DocumentsResponse> responseObserver) {
    this.request = request;
    this.connection = connection;
    this.docsQueryExecutor = docsQueryExecutor;
    this.bridgeService = bridgeService;
    this.responseObserver = responseObserver;
  }

  void handle() {
    Schema.DocumentsResponse.Builder response = Schema.DocumentsResponse.newBuilder();
    QueryOuterClass.Query q = request.getQuery();
    docsQueryExecutor.queryDocs(
        q.getCql(),
        request.getPageSize(),
        request.getExponentialSize(),
        request.getPageState().asReadOnlyByteBuffer());
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }
}
