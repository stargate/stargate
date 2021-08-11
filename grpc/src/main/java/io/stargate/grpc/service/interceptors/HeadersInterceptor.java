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
package io.stargate.grpc.service.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.stargate.grpc.service.Service;
import java.util.HashMap;
import java.util.Map;

public class HeadersInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Map<String, String> stringHeaders = new HashMap<>();
    for (String key : headers.keys()) {
      if (key.endsWith("-bin")) {
        continue;
      }
      String value = headers.get(Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
      stringHeaders.put(key, value);
    }
    Context context = Context.current();
    context = context.withValue(Service.HEADERS_KEY, stringHeaders);
    return Contexts.interceptCall(context, call, headers, next);
  }
}
