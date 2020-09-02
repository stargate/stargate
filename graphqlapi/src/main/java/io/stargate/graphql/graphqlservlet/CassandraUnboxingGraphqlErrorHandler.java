package io.stargate.graphql.graphqlservlet;

import graphql.ExceptionWhileDataFetching;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.kickstart.execution.error.GraphQLErrorHandler;

import java.util.ArrayList;
import java.util.List;

public class CassandraUnboxingGraphqlErrorHandler implements GraphQLErrorHandler {
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
            ExceptionWhileDataFetching fetchingError = (ExceptionWhileDataFetching) error;
            Throwable unboxed = unboxException(fetchingError.getException());
            return GraphqlErrorException.newErrorException()
                    .cause(unboxed)
                    .message(unboxed.getMessage())
                    .path(error.getPath())
                    .build();
        }
        return null;
    }
    public Throwable unboxException(Throwable exception) {
        while (exception.getCause() != null) {
            exception = exception.getCause();
        }

        return exception;
    }
}
