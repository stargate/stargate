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
import javax.annotation.Nullable;
import org.immutables.value.Value;

public class NewConnectionInterceptor implements ServerInterceptor {

  public static final Metadata.Key<String> TOKEN_KEY =
      Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);

  @Value.Immutable
  interface RequestInfo {

    String token();

    Map<String, String> headers();

    @Nullable
    SocketAddress remoteAddress();
  }

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final LoadingCache<RequestInfo, Connection> connectionCache =
      Caffeine.newBuilder()
          .maximumSize(10_000) // TODO: Make configurable?
          .expireAfterWrite(Duration.ofMinutes(1))
          .build(this::newConnection);

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

      Map<String, String> stringHeaders = new HashMap<>();
      for (String key : headers.keys()) {
        if (key.endsWith("-bin") || key.startsWith("grpc-")) {
          continue;
        }
        String value = headers.get(Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
        stringHeaders.put(key, value);
      }

      RequestInfo info =
          ImmutableRequestInfo.builder()
              .headers(stringHeaders)
              .token(token)
              .remoteAddress(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
              .build();

      Connection connection = connectionCache.get(info);

      Context context = Context.current();
      context = context.withValue(GrpcService.CONNECTION_KEY, connection);
      return Contexts.interceptCall(context, call, headers, next);
    } catch (Exception e) {
      call.close(
          Status.UNAUTHENTICATED.withDescription("Invalid token").withCause(e), new Metadata());
    }
    return new NopListener<>();
  }

  private Connection newConnection(RequestInfo info) throws UnauthorizedException {
    AuthenticationSubject authenticationSubject = authenticationService.validateToken(info.token());

    AuthenticatedUser user = authenticationSubject.asUser();
    Connection connection;
    if (!user.isFromExternalAuth()) {
      SocketAddress remoteAddress = info.remoteAddress();
      InetSocketAddress inetSocketAddress = DUMMY_ADDRESS;
      if (remoteAddress instanceof InetSocketAddress) {
        inetSocketAddress = (InetSocketAddress) remoteAddress;
      }
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

  private static class NopListener<ReqT> extends ServerCall.Listener<ReqT> {}
}
