package org.apache.cassandra.stargate.transport.internal;

import com.datastax.oss.driver.shaded.guava.common.base.Predicate;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.unix.Errors.NativeIoException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.FrameEncoder.Payload;
import org.apache.cassandra.net.FrameEncoder.PayloadAllocator;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHandlers {
  private static final Logger logger = LoggerFactory.getLogger(ExceptionHandlers.class);

  public ExceptionHandlers() {}

  public static ChannelInboundHandlerAdapter postV5Handler(
      PayloadAllocator allocator, ProtocolVersion version) {
    return new ExceptionHandlers.PostV5ExceptionHandler(allocator, version);
  }

  static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable> {
    private static final Set<String> ioExceptionsAtDebugLevel =
        ImmutableSet.of("Connection reset by peer", "Broken pipe", "Connection timed out");
    private final Channel channel;
    private final boolean alwaysLogAtError;

    UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError) {
      this.channel = channel;
      this.alwaysLogAtError = alwaysLogAtError;
    }

    public boolean apply(Throwable exception) {
      String message;
      try {
        message = "Unexpected exception during request; channel = " + this.channel;
      } catch (Exception var7) {
        message = "Unexpected exception during request; channel = <unprintable>";
      }

      if (!this.alwaysLogAtError
          && (exception instanceof IOException || exception.getCause() instanceof IOException)) {
        String errorMessage = exception.getMessage();
        boolean logAtTrace = false;
        Iterator var5 = ioExceptionsAtDebugLevel.iterator();

        while (var5.hasNext()) {
          String ioException = (String) var5.next();
          if (errorMessage.contains(ioException)) {
            logAtTrace = true;
            break;
          }
        }

        if (logAtTrace) {
          ExceptionHandlers.logger.trace(message, exception);
        } else {
          ExceptionHandlers.logger.info(message, exception);
        }
      } else {
        ExceptionHandlers.logger.error(message, exception);
      }

      return true;
    }
  }

  private static final class PostV5ExceptionHandler extends ChannelInboundHandlerAdapter {
    private final PayloadAllocator allocator;
    private final ProtocolVersion version;

    public PostV5ExceptionHandler(PayloadAllocator allocator, ProtocolVersion version) {
      this.allocator = allocator;
      this.version = version;
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (ctx.channel().isOpen()) {
        ExceptionHandlers.UnexpectedChannelExceptionHandler handler =
            new ExceptionHandlers.UnexpectedChannelExceptionHandler(ctx.channel(), false);
        ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
        Envelope response = errorMessage.encode(this.version);
        Payload payload =
            this.allocator.allocate(true, CQLMessageHandler.envelopeSize(response.header));

        try {
          response.encodeInto(payload.buffer);
          response.release();
          payload.finish();
          ChannelPromise promise = ctx.newPromise();
          if (isFatal(cause)) {
            promise.addListener(
                (future) -> {
                  ctx.close();
                });
          }

          ctx.writeAndFlush(payload, promise);
        } finally {
          payload.release();
          JVMStabilityInspector.inspectThrowable(cause);
        }
      }

      if (Throwables.anyCauseMatches(
          cause,
          (t) -> {
            return t instanceof ProtocolException;
          })) {
        if (Throwables.anyCauseMatches(
            cause,
            (t) -> {
              return t instanceof ProtocolException && !((ProtocolException) t).isSilent();
            })) {
          ClientMetrics.instance.markProtocolException();
          NoSpamLogger.log(
              ExceptionHandlers.logger,
              Level.WARN,
              1L,
              TimeUnit.MINUTES,
              "Protocol exception with client networking: " + cause.getMessage(),
              new Object[0]);
        }
      } else if (Throwables.anyCauseMatches(
          cause,
          (t) -> {
            return t instanceof NativeIoException;
          })) {
        ClientMetrics.instance.markUnknownException();
        ExceptionHandlers.logger.trace("Native exception in client networking", cause);
      } else {
        ClientMetrics.instance.markUnknownException();
        ExceptionHandlers.logger.warn("Unknown exception in client networking", cause);
      }
    }

    private static boolean isFatal(Throwable cause) {
      return Throwables.anyCauseMatches(
          cause,
          (t) -> {
            return t instanceof ProtocolException && ((ProtocolException) t).isFatal();
          });
    }
  }
}
