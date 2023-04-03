package io.stargate.db.cassandra.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.Message.Response;

class ReflectionUtils {

  private static final Method requestSetTracingRequested;
  private static final Method responseGetTracingId;
  private static final Constructor<? extends QueryOptions> optionsWithNamesCtor;

  static {
    try {
      requestSetTracingRequested = Request.class.getDeclaredMethod("setTracingRequested");
      requestSetTracingRequested.setAccessible(true);

      responseGetTracingId = Response.class.getDeclaredMethod("getTracingId");
      responseGetTracingId.setAccessible(true);

      // Note that the ctor for OptionsWithNames directly takes a DefaultQueryOptions which is not
      // accessible. That said, we know QueryOptions#create, which we'll use to build the object
      // passed as that argument, actually does create a DefaultQueryOptions, so we're good.
      Class<?> defaultOptionsClass =
          Class.forName("org.apache.cassandra.cql3.QueryOptions$DefaultQueryOptions");
      @SuppressWarnings("unchecked")
      Class<? extends QueryOptions> withNamesClass =
          (Class<? extends QueryOptions>)
              Class.forName("org.apache.cassandra.cql3.QueryOptions$OptionsWithNames");

      optionsWithNamesCtor = withNamesClass.getDeclaredConstructor(defaultOptionsClass, List.class);
      optionsWithNamesCtor.setAccessible(true);
    } catch (Exception e) {
      // We know it's there.
      throw new AssertionError(
          "Error during initialization of the persistence layer: "
              + "some reflection-based accesses cannot be setup.");
    }
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  private static <T> T invoke(Method method, Object target, Object... args) {
    try {
      Object result = method.invoke(target, args);
      return result == null ? null : (T) result;
    } catch (IllegalAccessException e) {
      throw new AssertionError();
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw new RuntimeException(e);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private static <T> T newInstance(Constructor<T> ctor, Object... args) {
    try {
      return ctor.newInstance(args);
    } catch (IllegalAccessException e) {
      throw new AssertionError();
    } catch (InstantiationException | InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw new RuntimeException(e);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  static void setTracingRequested(Request request) {
    invoke(requestSetTracingRequested, request);
  }

  static UUID getTracingId(Response response) {
    return invoke(responseGetTracingId, response);
  }

  static QueryOptions newOptionsWithNames(QueryOptions options, List<String> boundNames) {
    return newInstance(optionsWithNamesCtor, options, boundNames);
  }
}
