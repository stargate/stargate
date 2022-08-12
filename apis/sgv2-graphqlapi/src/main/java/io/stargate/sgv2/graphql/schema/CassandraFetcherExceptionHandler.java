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
package io.stargate.sgv2.graphql.schema;

import graphql.ExceptionWhileDataFetching;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.SimpleDataFetcherExceptionHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public class CassandraFetcherExceptionHandler extends SimpleDataFetcherExceptionHandler {

  public static CassandraFetcherExceptionHandler INSTANCE = new CassandraFetcherExceptionHandler();

  @Override
  public DataFetcherExceptionHandlerResult onException(
      DataFetcherExceptionHandlerParameters handlerParameters) {

    if (isOverloaded(handlerParameters.getException())) {
      StargateGraphqlContext context = handlerParameters.getDataFetchingEnvironment().getContext();
      context.setOverloaded();
    }

    return super.onException(handlerParameters);
  }

  @Override
  protected void logException(ExceptionWhileDataFetching error, Throwable exception) {
    // 24-Jun-2022, tatu: [stargate#1279] Since we handle exception at an outer level,
    //    should not log it here.
  }

  private boolean isOverloaded(Throwable t) {
    if (t instanceof StatusRuntimeException e) {
      // This is how the bridge reports Cassandra OVERLOADED errors.
      return e.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED;
    }
    return false;
  }
}
