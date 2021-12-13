package io.grpc.netty.shaded.io.grpc.netty;

import static io.stargate.grpc.service.GrpcService.CONNECTION_KEY;

import io.grpc.internal.SharedResourcePool;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFuture;
import io.grpc.netty.shaded.io.netty.channel.ChannelPromise;
import io.grpc.netty.shaded.io.netty.channel.EventLoop;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.group.ChannelGroup;
import io.grpc.netty.shaded.io.netty.channel.group.DefaultChannelGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.EventExecutor;
import io.grpc.netty.shaded.io.netty.util.concurrent.Future;
import io.grpc.netty.shaded.io.netty.util.concurrent.GlobalEventExecutor;
import io.grpc.netty.shaded.io.netty.util.concurrent.ScheduledFuture;
import io.stargate.grpc.service.GrpcService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** It is a pass-through decorator. */
public class CustomEventLoopGroup implements EventLoopGroup {
  private static final Logger logger = LoggerFactory.getLogger(CustomEventLoopGroup.class);
  private final List<Channel> channels = new ArrayList<>();
  private final SharedResourcePool<EventLoopGroup> group;

  public CustomEventLoopGroup(SharedResourcePool<EventLoopGroup> group) {
    this.group = group;
  }

  public static CustomEventLoopGroup worker() {
    SharedResourcePool<EventLoopGroup> group =
        SharedResourcePool.forResource(Utils.DEFAULT_WORKER_EVENT_LOOP_GROUP);
    return new CustomEventLoopGroup(group);
  }

  public static CustomEventLoopGroup boss() {
    SharedResourcePool<EventLoopGroup> group =
        SharedResourcePool.forResource(Utils.DEFAULT_BOSS_EVENT_LOOP_GROUP);
    return new CustomEventLoopGroup(group);
  }

  @Override
  public boolean isShuttingDown() {
    return group.getObject().isShuttingDown();
  }

  @Override
  public Future<?> shutdownGracefully() {
    return group.getObject().shutdownGracefully();
  }

  @Override
  public Future<?> shutdownGracefully(long l, long l1, TimeUnit timeUnit) {
    return group.getObject().shutdownGracefully(l, l1, timeUnit);
  }

  @Override
  public Future<?> terminationFuture() {
    return group.getObject().terminationFuture();
  }

  @Override
  public void shutdown() {
    group.getObject().shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return group.getObject().shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return group.getObject().isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return group.getObject().isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException {
    return group.getObject().awaitTermination(timeout, unit);
  }

  @Override
  public EventLoop next() {
    return group.getObject().next();
  }

  @Override
  public Iterator<EventExecutor> iterator() {
    return group.getObject().iterator();
  }

  @Override
  public Future<?> submit(Runnable runnable) {
    return group.getObject().submit(runnable);
  }

  @NotNull
  @Override
  public <T> List<java.util.concurrent.Future<T>> invokeAll(
      @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return group.getObject().invokeAll(tasks);
  }

  @NotNull
  @Override
  public <T> List<java.util.concurrent.Future<T>> invokeAll(
      @NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit)
      throws InterruptedException {
    return group.getObject().invokeAll(tasks, timeout, unit);
  }

  @NotNull
  @Override
  public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return group.getObject().invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(
      @NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return group.getObject().invokeAny(tasks, timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T t) {
    return group.getObject().submit(runnable, t);
  }

  @Override
  public <T> Future<T> submit(Callable<T> callable) {
    return group.getObject().submit(callable);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long l, TimeUnit timeUnit) {
    return group.getObject().schedule(runnable, l, timeUnit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long l, TimeUnit timeUnit) {
    return group.getObject().schedule(callable, l, timeUnit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable runnable, long l, long l1, TimeUnit timeUnit) {
    return group.getObject().scheduleAtFixedRate(runnable, l, l1, timeUnit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable runnable, long l, long l1, TimeUnit timeUnit) {
    return group.getObject().scheduleAtFixedRate(runnable, l, l1, timeUnit);
  }

  @Override
  public ChannelFuture register(Channel channel) {
    channels.add(channel);
    System.out.println(
        "register(Channel channel) "
            + channel.id()
            + " type: "
            + channel.getClass()
            + " thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());

    System.out.println(
        "register(Channel channel), CONNECTION_KEY.get(): "
            + CONNECTION_KEY.get()
            + "headers:"
            + GrpcService.HEADERS_KEY
            + "thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());
    return group.getObject().register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise channelPromise) {
    channels.add(channelPromise.channel());
    System.out.println(
        "register(ChannelPromise channelPromise) "
            + channelPromise.channel().id()
            + " thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());
    return group.getObject().register(channelPromise);
  }

  @Override
  public ChannelFuture register(Channel channel, ChannelPromise channelPromise) {
    channels.add(channel);
    System.out.println(
        "register(Channel channel, ChannelPromise channelPromise) "
            + channel.id()
            + " thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());
    return group.getObject().register(channel, channelPromise);
  }

  @Override
  public void execute(@NotNull Runnable command) {
    group.getObject().execute(command);
  }

  public void closeFilter(Predicate<Map<String, String>> headerFilter) {
    ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    channels.stream()
        .filter(
            channel -> {
              Map<String, String> headersFromContext = GrpcService.HEADERS_KEY.get();
              Map<String, String> headers =
                  headersFromContext != null ? headersFromContext : Collections.emptyMap();
              logger.info(
                  "CustomEventLoopGroup.closeFilter(): nr of channels: "
                      + channels.size()
                      + " headers: "
                      + headers
                      + "Thread: "
                      + Thread.currentThread().getName());
              return headerFilter.test(headers);
            })
        .forEach(allChannels::add);
    writeToGroup(allChannels);
  }

  private void writeToGroup(ChannelGroup allChannels) {
    logger.info("Closing channels, number of channels: {} ", allChannels.size());
    GracefulServerCloseCommand streamClosed = new GracefulServerCloseCommand("Stream closed");
    allChannels.writeAndFlush(streamClosed).awaitUninterruptibly();
    allChannels.close().awaitUninterruptibly();
  }
}
