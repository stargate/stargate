package io.stargate.it.grpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;

public class StargateBearerToken extends CallCredentials {
  public static final Metadata.Key<String> TOKEN_KEY =
      Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

  private final String token;

  public StargateBearerToken(String token) {
    this.token = token;
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        () -> {
          try {
            Metadata metadata = new Metadata();
            metadata.put(TOKEN_KEY, token);
            applier.apply(metadata);
          } catch (Exception e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }

  @Override
  public void thisUsesUnstableApi() {}
}
