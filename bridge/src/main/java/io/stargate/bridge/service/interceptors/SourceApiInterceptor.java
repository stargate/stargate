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

package io.stargate.bridge.service.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.stargate.auth.SourceAPI;
import io.stargate.bridge.service.BridgeService;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Bridge interceptor that reads the source API from the metadata and adds it to the context. */
public class SourceApiInterceptor implements ServerInterceptor {

  public static final Metadata.Key<String> SOURCE_API_KEY =
      Metadata.Key.of("X-Source-Api", Metadata.ASCII_STRING_MARSHALLER);

  /** If request should fails on missing source api. */
  private final boolean failOnMissing;

  /** @param failOnMissing If request should fails on missing source api. */
  public SourceApiInterceptor(boolean failOnMissing) {
    this.failOnMissing = failOnMissing;
  }

  /** {@inheritDoc} */
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    // get the source api and ensure value is valid
    String headerValue = headers.get(SOURCE_API_KEY);
    Optional<SourceAPI> sourceApi =
        Arrays.stream(SourceAPI.values())
            .filter(s -> Objects.equals(s.getName(), headerValue))
            .findFirst();

    // if present set and continue
    if (sourceApi.isPresent()) {
      Context context = Context.current().withValue(BridgeService.SOURCE_API_KEY, sourceApi.get());
      return Contexts.interceptCall(context, call, headers, next);
    }

    // if not present, and required must fail
    if (failOnMissing) {
      String msg =
          String.format(
              "Source API can not be found in metadata, expecting in the value in the X-Source-Api. Received: %s",
              headerValue);
      call.close(Status.INVALID_ARGUMENT.withDescription(msg), new Metadata());
      return new ServerCall.Listener<ReqT>() {};
    }

    // otherwise just continue
    return next.startCall(call, headers);
  }
}
