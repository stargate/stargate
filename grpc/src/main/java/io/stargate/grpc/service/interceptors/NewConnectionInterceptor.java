package io.stargate.grpc.service.interceptors;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.grpc.service.GrpcService;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.immutables.value.Value;

public class NewConnectionInterceptor implements ServerInterceptor {

  public static final Metadata.Key<String> TOKEN_KEY =
      Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);
  private static final int CACHE_TTL_SECS =
      Integer.getInteger("stargate.grpc.connection_cache_ttl_seconds", 60);
  private static final int CACHE_MAX_SIZE =
      Integer.getInteger("stargate.grpc.connection_cache_max_size", 10_000);

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final LoadingCache<RequestInfo, Connection> connectionCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECS))
          .maximumSize(CACHE_MAX_SIZE)
          .build(this::newConnection);

  @Value.Immutable
  interface RequestInfo {

    String token();

    Map<String, String> headers();

    @Nullable
    SocketAddress remoteAddress();
  }

  public NewConnectionInterceptor(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    try {
      String token = headers.get(TOKEN_KEY);
      if (token == null) {
        call.close(Status.UNAUTHENTICATED.withDescription("No token provided"), new Metadata());
        return new NopListener<>();
      }

      Map<String, String> stringHeaders = convertAndFilterHeaders(headers);
      RequestInfo info =
          ImmutableRequestInfo.builder()
              .token(token)
              .headers(stringHeaders)
              .remoteAddress(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
              .build();

      Connection connection = connectionCache.get(info);

      Context context = Context.current();
      context =
          context.withValues(
              GrpcService.CONNECTION_KEY, connection, GrpcService.HEADERS_KEY, stringHeaders);
      return Contexts.interceptCall(context, call, headers, next);
    } catch (Exception e) {
      Throwable cause = e;
      if (cause instanceof CompletionException) {
        cause = e.getCause();
      }
      if (cause instanceof UnauthorizedException) {
        call.close(
            Status.UNAUTHENTICATED.withDescription("Invalid token").withCause(e), new Metadata());
      } else if (cause instanceof UnhandledClientException) {
        call.close(Status.UNAVAILABLE.withDescription(e.getMessage()).withCause(e), new Metadata());
      } else {
        call.close(
            Status.INTERNAL
                .withDescription("Error attempting to create connection to persistence")
                .withCause(e),
            new Metadata());
      }
    }
    return new NopListener<>();
  }

  private Connection newConnection(RequestInfo info) throws UnauthorizedException {
    AuthenticationSubject authenticationSubject =
        authenticationService.validateToken(info.token(), info.headers());

    AuthenticatedUser user = authenticationSubject.asUser();
    Connection connection;
    if (!user.isFromExternalAuth()) {
      SocketAddress remoteAddress = info.remoteAddress();
      // This is best effort attempt to set the remote address, if the remote address is not the
      // correct type then use a dummy value. Note: `remoteAddress` is almost always a
      // `InetSocketAddress`.
      InetSocketAddress inetSocketAddress =
          remoteAddress instanceof InetSocketAddress
              ? (InetSocketAddress) remoteAddress
              : DUMMY_ADDRESS;
      connection = persistence.newConnection(new ClientInfo(inetSocketAddress, null));
    } else {
      connection = persistence.newConnection();
    }
    connection.login(user);
    if (user.token() != null) {
      connection.clientInfo().ifPresent(c -> c.setAuthenticatedUser(user));
    }
    connection.setCustomProperties(info.headers());
    return connection;
  }

  private static Map<String, String> convertAndFilterHeaders(Metadata headers) {
    Map<String, String> stringHeaders = new HashMap<>();
    for (String key : headers.keys()) {
      // Ignore gRPC specific (in addition to binary) headers because they're not used by Connection
      // and some of the values contain unique values for each request which prevents effective
      // caching.
      if (key.endsWith("-bin") || key.startsWith("grpc-")) {
        continue;
      }
      String value = headers.get(Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
      stringHeaders.put(key, value);
    }
    return stringHeaders;
  }

  private static class NopListener<ReqT> extends ServerCall.Listener<ReqT> {}
}
