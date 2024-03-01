package io.stargate.db.dse.impl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerConnection;

// This is a bit of a monstrosity, but should be pretty temporary. This work around the fact that
// that in DSE (but not in C*) some (tracing) code uses Message#getRemoteAddress(), which accesses
// the remote address from the underlying ServerConnection (that gets attached to the Message).
// Exception that this Message#getRemoteAddress(), while protected, is _final_, so we can't override
// it as of now (at least I don't think so, not with bytecode manipulation).
// And creating a genuine ServerConnection is pretty involved because encapsulate the actual
// network Netty Channel, but we don't have that. Hence that class which define just enough that
// we can build a fake ServerConnection object that has just enough so that 1) it can be
// constructed without error and 2) return the remote address we want on getRemoteAddress().
// Do note that this should go away soon. First, we'll change that Message#getRemoteAddress()
// method to not be final (protected and final is a weird combination in the first place), but
// even without that, the tracing code that call that method probably shouldn't: instead they
// should reach into the QueryState that they have access to, as done by the C* code an plenty
// other places.
public class FakeConnection extends ServerConnection {
  public FakeConnection(InetSocketAddress remoteAddress, ProtocolVersion version) {
    super(new FakeChannel(remoteAddress), version, new FakeTracker(), false);
  }

  private static class FakeChannel implements Channel {

    private final InetSocketAddress remoteAddress;

    private FakeChannel(InetSocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }

    @Override
    public ChannelId id() {
      return null;
    }

    @Override
    public EventLoop eventLoop() {
      return null;
    }

    @Override
    public Channel parent() {
      return null;
    }

    @Override
    public ChannelConfig config() {
      return null;
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public boolean isRegistered() {
      return false;
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public ChannelMetadata metadata() {
      return null;
    }

    @Override
    public SocketAddress localAddress() {
      return null;
    }

    @Override
    public SocketAddress remoteAddress() {
      return remoteAddress;
    }

    @Override
    public ChannelFuture closeFuture() {
      return new ChannelFuture() {
        @Override
        public Channel channel() {
          return FakeChannel.this;
        }

        @Override
        public ChannelFuture addListener(
            GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
          return null;
        }

        @Override
        public ChannelFuture addListeners(
            GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
          return null;
        }

        @Override
        public ChannelFuture removeListener(
            GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
          return null;
        }

        @Override
        public ChannelFuture removeListeners(
            GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
          return null;
        }

        @Override
        public ChannelFuture sync() throws InterruptedException {
          return null;
        }

        @Override
        public ChannelFuture syncUninterruptibly() {
          return null;
        }

        @Override
        public ChannelFuture await() throws InterruptedException {
          return null;
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
          return null;
        }

        @Override
        public boolean isVoid() {
          return false;
        }

        @Override
        public boolean isSuccess() {
          return false;
        }

        @Override
        public boolean isCancellable() {
          return false;
        }

        @Override
        public Throwable cause() {
          return null;
        }

        @Override
        public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
          return false;
        }

        @Override
        public boolean await(long l) throws InterruptedException {
          return false;
        }

        @Override
        public boolean awaitUninterruptibly(long l, TimeUnit timeUnit) {
          return false;
        }

        @Override
        public boolean awaitUninterruptibly(long l) {
          return false;
        }

        @Override
        public Void getNow() {
          return null;
        }

        @Override
        public boolean cancel(boolean b) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return false;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
          return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
          return null;
        }
      };
    }

    @Override
    public boolean isWritable() {
      return false;
    }

    @Override
    public long bytesBeforeUnwritable() {
      return 0;
    }

    @Override
    public long bytesBeforeWritable() {
      return 0;
    }

    @Override
    public Unsafe unsafe() {
      return null;
    }

    @Override
    public ChannelPipeline pipeline() {
      return null;
    }

    @Override
    public ByteBufAllocator alloc() {
      return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress) {
      return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress) {
      return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
      return null;
    }

    @Override
    public ChannelFuture disconnect() {
      return null;
    }

    @Override
    public ChannelFuture close() {
      return null;
    }

    @Override
    public ChannelFuture deregister() {
      return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture connect(
        SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture close(ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture deregister(ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public Channel read() {
      return null;
    }

    @Override
    public ChannelFuture write(Object o) {
      return null;
    }

    @Override
    public ChannelFuture write(Object o, ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public Channel flush() {
      return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
      return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o) {
      return null;
    }

    @Override
    public ChannelPromise newPromise() {
      return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
      return null;
    }

    @Override
    public ChannelFuture newSucceededFuture() {
      return null;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable throwable) {
      return null;
    }

    @Override
    public ChannelPromise voidPromise() {
      return null;
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
      return new Attribute<T>() {
        @Override
        public AttributeKey<T> key() {
          return null;
        }

        @Override
        public T get() {
          return null;
        }

        @Override
        public void set(T t) {}

        @Override
        public T getAndSet(T t) {
          return null;
        }

        @Override
        public T setIfAbsent(T t) {
          return null;
        }

        @Override
        public T getAndRemove() {
          return null;
        }

        @Override
        public boolean compareAndSet(T t, T t1) {
          return false;
        }

        @Override
        public void remove() {}
      };
    }

    @Override
    public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
      return false;
    }

    @Override
    public int compareTo(Channel o) {
      return 0;
    }
  }

  private static class FakeTracker implements Tracker {

    @Override
    public void addConnection(Channel channel, Connection connection) {}

    @Override
    public void removeConnection(Channel channel, Connection connection) {}
  }
}
