package io.stargate.bridge.service;

import io.grpc.stub.StreamObserver;

/**
 * It wraps the underlying {@link StreamObserver} and delegates all the method to it. Additionally,
 * it adds a synchronization on the {@code StreamObserver} because, as stated in the <a
 * href="https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html">StreamObserver
 * docs</a>:
 *
 * <pre>
 * Since individual StreamObservers are not thread-safe,
 * if multiple threads will bewriting to a StreamObserver concurrently,
 * the application must synchronize calls.
 * </pre>
 *
 * all calls need to be synchronized.
 *
 * @param <V>
 */
public class SynchronizedStreamObserver<V> implements StreamObserver<V> {

  private final StreamObserver<V> streamObserver;

  public SynchronizedStreamObserver(StreamObserver<V> streamObserver) {
    this.streamObserver = streamObserver;
  }

  @Override
  public void onNext(V value) {
    synchronized (streamObserver) {
      streamObserver.onNext(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    synchronized (streamObserver) {
      streamObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    synchronized (streamObserver) {
      streamObserver.onCompleted();
    }
  }
}
