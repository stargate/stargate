package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.internal.SharedResourcePool;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFuture;
import io.grpc.netty.shaded.io.netty.channel.ChannelPromise;
import io.grpc.netty.shaded.io.netty.channel.EventLoop;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.EventExecutor;
import io.grpc.netty.shaded.io.netty.util.concurrent.Future;
import io.grpc.netty.shaded.io.netty.util.concurrent.ScheduledFuture;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public class CustomEventLoopGroup implements EventLoopGroup {

  private final SharedResourcePool<EventLoopGroup> group;

  public CustomEventLoopGroup(SharedResourcePool<EventLoopGroup> group) {
    this.group = group;
  }

  public static EventLoopGroup worker() {
    SharedResourcePool<EventLoopGroup> group =
        SharedResourcePool.forResource(Utils.DEFAULT_WORKER_EVENT_LOOP_GROUP);
    return new CustomEventLoopGroup(group);
  }

  public static EventLoopGroup boss() {
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
    return group.getObject().register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise channelPromise) {
    return group.getObject().register(channelPromise);
  }

  @Override
  public ChannelFuture register(Channel channel, ChannelPromise channelPromise) {
    return group.getObject().register(channel, channelPromise);
  }

  @Override
  public void execute(@NotNull Runnable command) {
    group.getObject().execute(command);
  }
}
