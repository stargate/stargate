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

import com.github.benmanes.caffeine.cache.Cache;
import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Prepared;
import io.stargate.grpc.service.Service.PrepareInfo;
import io.stargate.proto.QueryOuterClass.Response;
import java.util.concurrent.CompletionStage;

class BatchHandler extends MessageHandler {

  protected BatchHandler(
      Connection connection,
      Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    super(connection, preparedCache, persistence, responseObserver);
  }

  // TODO
}
