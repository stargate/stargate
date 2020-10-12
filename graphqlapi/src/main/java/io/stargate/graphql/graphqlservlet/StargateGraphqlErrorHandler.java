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
package io.stargate.graphql.graphqlservlet;

import graphql.ExceptionWhileDataFetching;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.kickstart.execution.error.GraphQLErrorHandler;
import java.util.ArrayList;
import java.util.List;

public class StargateGraphqlErrorHandler implements GraphQLErrorHandler {

  @Override
  public List<GraphQLError> processErrors(List<GraphQLError> errors) {
    List<GraphQLError> unboxed = new ArrayList<>();
    for (GraphQLError error : errors) {
      GraphQLError newError = unbox(error);
      if (newError != null) {
        unboxed.add(newError);
      }
    }
    return unboxed;
  }

  private GraphQLError unbox(GraphQLError error) {
    if (error instanceof ExceptionWhileDataFetching) {
      // Hide the stack trace to the client
      ExceptionWhileDataFetching fetchingError = (ExceptionWhileDataFetching) error;
      Throwable unboxed = unboxException(fetchingError.getException());
      return GraphqlErrorException.newErrorException()
          .cause(unboxed)
          .message(unboxed.getMessage())
          .path(error.getPath())
          .build();
    }

    // ValidationError, TypeMismatchError, ... should not be unboxed
    return error;
  }

  public Throwable unboxException(Throwable exception) {
    while (exception.getCause() != null) {
      exception = exception.getCause();
    }

    return exception;
  }
}
