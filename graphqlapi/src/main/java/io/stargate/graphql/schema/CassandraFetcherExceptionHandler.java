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
package io.stargate.graphql.schema;

import graphql.ExceptionWhileDataFetching;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.SimpleDataFetcherExceptionHandler;
import io.stargate.graphql.web.StargateGraphqlContext;
import org.apache.cassandra.stargate.exceptions.OverloadedException;

public class CassandraFetcherExceptionHandler extends SimpleDataFetcherExceptionHandler {

  public static CassandraFetcherExceptionHandler INSTANCE = new CassandraFetcherExceptionHandler();

  @Override
  public DataFetcherExceptionHandlerResult onException(
      DataFetcherExceptionHandlerParameters handlerParameters) {
    if (handlerParameters.getException() instanceof OverloadedException) {
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
}
