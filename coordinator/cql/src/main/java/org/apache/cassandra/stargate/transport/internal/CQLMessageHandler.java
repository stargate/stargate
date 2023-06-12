//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.cassandra.stargate.transport.internal;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMessageSizeMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.net.ResourceLimits.Outcome;
import org.apache.cassandra.net.ShareableBytes;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQLMessageHandler<M extends Message> extends AbstractMessageHandler {
  private static final Logger logger = LoggerFactory.getLogger(CQLMessageHandler.class);
  private static final NoSpamLogger noSpamLogger;
  public static final int LARGE_MESSAGE_THRESHOLD = 131071;
  private final Envelope.Decoder envelopeDecoder;
  private final Message.Decoder<M> messageDecoder;
  private final FrameEncoder.PayloadAllocator payloadAllocator;
  private final MessageConsumer<M> dispatcher;
  private final ErrorHandler errorHandler;
  private final boolean throwOnOverload;
  private final ProtocolVersion version;
  long channelPayloadBytesInFlight;
  private int consecutiveMessageErrors = 0;

  CQLMessageHandler(
      Channel channel,
      ProtocolVersion version,
      FrameDecoder decoder,
      Envelope.Decoder envelopeDecoder,
      Message.Decoder<M> messageDecoder,
      MessageConsumer<M> dispatcher,
      FrameEncoder.PayloadAllocator payloadAllocator,
      int queueCapacity,
      ClientResourceLimits.ResourceProvider resources,
      AbstractMessageHandler.OnHandlerClosed onClosed,
      ErrorHandler errorHandler,
      boolean throwOnOverload) {
    super(
        decoder,
        channel,
        131071,
        (long) queueCapacity,
        resources.endpointLimit(),
        resources.globalLimit(),
        resources.endpointWaitQueue(),
        resources.globalWaitQueue(),
        onClosed);
    this.envelopeDecoder = envelopeDecoder;
    this.messageDecoder = messageDecoder;
    this.payloadAllocator = payloadAllocator;
    this.dispatcher = dispatcher;
    this.errorHandler = errorHandler;
    this.throwOnOverload = throwOnOverload;
    this.version = version;
  }

  public boolean process(FrameDecoder.Frame frame) throws IOException {
    this.consecutiveMessageErrors = 0;
    return super.process(frame);
  }

  protected boolean processOneContainedMessage(
      ShareableBytes bytes,
      ResourceLimits.Limit endpointReserve,
      ResourceLimits.Limit globalReserve) {
    ByteBuffer buf = bytes.get();
    Envelope.Decoder.HeaderExtractionResult extracted = this.envelopeDecoder.extractHeader(buf);
    if (!extracted.isSuccess()) {
      return this.handleProtocolException(
          extracted.error(), buf, extracted.streamId(), extracted.bodyLength());
    } else {
      Envelope.Header header = extracted.header();
      if (header.version != this.version) {
        ProtocolException error =
            new ProtocolException(
                String.format(
                    "Invalid message version. Got %s but previousmessages on this connection had version %s",
                    header.version, this.version));
        return this.handleProtocolException(error, buf, header.streamId, header.bodySizeInBytes);
      } else {
        int messageSize = Ints.checkedCast(header.bodySizeInBytes);
        if (this.throwOnOverload) {
          if (!this.acquireCapacity(header, endpointReserve, globalReserve)) {
            ClientMetrics.instance.markRequestDiscarded();
            logger.trace(
                "Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
                new Object[] {
                  messageSize,
                  this.channelPayloadBytesInFlight,
                  endpointReserve.using(),
                  globalReserve.using(),
                  header
                });
            this.handleError(
                new OverloadedException(
                    "Server is in overloaded state. Cannot accept more requests at this point"),
                header);
            this.incrementReceivedMessageMetrics(messageSize);
            buf.position(buf.position() + 9 + messageSize);
            return true;
          }
        } else if (!this.acquireCapacityAndQueueOnFailure(header, endpointReserve, globalReserve)) {
          ClientMetrics.instance.pauseConnection();
          return false;
        }

        this.channelPayloadBytesInFlight += (long) messageSize;
        this.incrementReceivedMessageMetrics(messageSize);
        return this.processRequest(this.composeRequest(header, bytes));
      }
    }
  }

  private boolean handleProtocolException(
      ProtocolException exception, ByteBuffer buf, int streamId, long expectedMessageLength) {
    if (expectedMessageLength >= 0L
        && ++this.consecutiveMessageErrors
            <= DatabaseDescriptor.getConsecutiveMessageErrorsThreshold()) {
      this.handleError(exception, streamId);
      buf.position(
          Math.min(buf.limit(), buf.position() + 9 + Ints.checkedCast(expectedMessageLength)));
      return true;
    } else {
      if (!exception.isFatal()) {
        exception = ProtocolException.toFatalException(exception);
      }

      this.handleError(exception, streamId);
      return false;
    }
  }

  private void incrementReceivedMessageMetrics(int messageSize) {
    ++this.receivedCount;
    this.receivedBytes += (long) (messageSize + 9);
    ClientMessageSizeMetrics.bytesReceived.inc((long) (messageSize + 9));
    ClientMessageSizeMetrics.bytesReceivedPerRequest.update(messageSize + 9);
  }

  private Envelope composeRequest(Envelope.Header header, ShareableBytes bytes) {
    ByteBuffer buf = bytes.get();
    int idx = buf.position() + 9;
    int end = idx + Ints.checkedCast(header.bodySizeInBytes);
    ByteBuf body = Unpooled.wrappedBuffer(buf.slice());
    body.readerIndex(9);
    body.retain();
    buf.position(end);
    return new Envelope(header, body);
  }

  protected boolean processRequest(Envelope request) {
    M message = null;

    try {
      message = this.messageDecoder.decode(this.channel, request);
      this.dispatcher.accept(this.channel, message, this::toFlushItem);
      this.consecutiveMessageErrors = 0;
      return true;
    } catch (Exception var5) {
      Exception e = var5;
      if (message != null) {
        request.release();
      }

      boolean continueProcessing = true;
      if (++this.consecutiveMessageErrors
          > DatabaseDescriptor.getConsecutiveMessageErrorsThreshold()) {
        if (!(var5 instanceof ProtocolException)) {
          logger.debug("Error decoding CQL message", var5);
          e = new ProtocolException("Error encountered decoding CQL message: " + var5.getMessage());
        }

        e = ProtocolException.toFatalException((ProtocolException) e);
        continueProcessing = false;
      }

      this.handleErrorAndRelease((Throwable) e, request.header);
      return continueProcessing;
    }
  }

  private void handleErrorAndRelease(Throwable t, Envelope.Header header) {
    this.release(header);
    this.handleError(t, header);
  }

  private void handleError(Throwable t, Envelope.Header header) {
    this.handleError(t, header.streamId);
  }

  private void handleError(Throwable t, int streamId) {
    this.errorHandler.accept(ErrorMessage.wrap(t, streamId));
  }

  private void handleError(Throwable t) {
    this.errorHandler.accept(t);
  }

  private Flusher.FlushItem.Framed toFlushItem(
      Channel channel, Message.Request request, Message.Response response) {
    Envelope responseFrame = response.encode(request.getSource().header.version);
    int responseSize = envelopeSize(responseFrame.header);
    ClientMessageSizeMetrics.bytesSent.inc((long) responseSize);
    ClientMessageSizeMetrics.bytesSentPerResponse.update(responseSize);
    return new Flusher.FlushItem.Framed(
        channel, responseFrame, request.getSource(), this.payloadAllocator, this::release);
  }

  private void release(Flusher.FlushItem<Envelope> flushItem) {
    this.release(flushItem.request.header);
    flushItem.request.release();
    ((Envelope) flushItem.response).release();
  }

  private void release(Envelope.Header header) {
    this.releaseCapacity(Ints.checkedCast(header.bodySizeInBytes));
    this.channelPayloadBytesInFlight -= header.bodySizeInBytes;
  }

  protected boolean processFirstFrameOfLargeMessage(
      FrameDecoder.IntactFrame frame,
      ResourceLimits.Limit endpointReserve,
      ResourceLimits.Limit globalReserve)
      throws IOException {
    ShareableBytes bytes = frame.contents;
    ByteBuffer buf = bytes.get();

    try {
      Envelope.Decoder.HeaderExtractionResult extracted = this.envelopeDecoder.extractHeader(buf);
      if (!extracted.isSuccess()) {
        this.handleError(ProtocolException.toFatalException(extracted.error()));
        return false;
      } else {
        Envelope.Header header = extracted.header();
        int messageSize = Ints.checkedCast(header.bodySizeInBytes);
        this.receivedBytes += (long) buf.remaining();
        CQLMessageHandler<M>.LargeMessage largeMessage = new LargeMessage(header);
        if (!this.acquireCapacity(header, endpointReserve, globalReserve) && this.throwOnOverload) {
          ClientMetrics.instance.markRequestDiscarded();
          logger.trace(
              "Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Header: {}",
              new Object[] {
                messageSize,
                this.channelPayloadBytesInFlight,
                endpointReserve.using(),
                globalReserve.using(),
                header
              });
          largeMessage.markOverloaded();
        }

        this.largeMessage = largeMessage;
        largeMessage.supply(frame);
        return true;
      }
    } catch (Exception var10) {
      throw new IOException("Error decoding CQL Message", var10);
    }
  }

  protected String id() {
    return this.channel.id().asShortText();
  }

  private boolean acquireCapacityAndQueueOnFailure(
      Envelope.Header header,
      ResourceLimits.Limit endpointReserve,
      ResourceLimits.Limit globalReserve) {
    int bytesRequired = Ints.checkedCast(header.bodySizeInBytes);
    long currentTimeNanos = MonotonicClock.approxTime.now();
    return this.acquireCapacity(
        endpointReserve, globalReserve, bytesRequired, currentTimeNanos, Long.MAX_VALUE);
  }

  private boolean acquireCapacity(
      Envelope.Header header,
      ResourceLimits.Limit endpointReserve,
      ResourceLimits.Limit globalReserve) {
    int bytesRequired = Ints.checkedCast(header.bodySizeInBytes);
    return this.acquireCapacity(endpointReserve, globalReserve, bytesRequired) == Outcome.SUCCESS;
  }

  protected void processCorruptFrame(FrameDecoder.CorruptFrame frame) {
    ++this.corruptFramesUnrecovered;
    String error =
        String.format(
            "%s invalid, unrecoverable CRC mismatch detected in frame %s. Read %d, Computed %d",
            this.id(), frame.isRecoverable() ? "body" : "header", frame.readCRC, frame.computedCRC);
    noSpamLogger.error(error, new Object[0]);
    if (!frame.isSelfContained) {
      if (null == this.largeMessage) {
        this.receivedBytes += (long) frame.frameSize;
      } else {
        this.processSubsequentFrameOfLargeMessage(frame);
      }
    }

    this.handleError(ProtocolException.toFatalException(new ProtocolException(error)));
  }

  protected void fatalExceptionCaught(Throwable cause) {
    this.decoder.discard();
    logger.warn(
        "Unrecoverable exception caught in CQL message processing pipeline, closing the connection",
        cause);
    this.channel.close();
  }

  static int envelopeSize(Envelope.Header header) {
    return 9 + Ints.checkedCast(header.bodySizeInBytes);
  }

  static {
    noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);
  }

  private class LargeMessage extends AbstractMessageHandler.LargeMessage<Envelope.Header> {
    private static final long EXPIRES_AT = Long.MAX_VALUE;
    private boolean overloaded;

    private LargeMessage(Envelope.Header header) {
      super(CQLMessageHandler.envelopeSize(header), header, Long.MAX_VALUE, false);
      this.overloaded = false;
    }

    private Envelope assembleFrame() {
      ByteBuf body =
          Unpooled.wrappedBuffer(
              (ByteBuffer[])
                  this.buffers.stream()
                      .map(ShareableBytes::get)
                      .toArray(
                          (x$0) -> {
                            return new ByteBuffer[x$0];
                          }));
      body.readerIndex(9);
      body.retain();
      return new Envelope((Envelope.Header) this.header, body);
    }

    private void markOverloaded() {
      this.overloaded = true;
    }

    protected void onComplete() {
      if (this.overloaded) {
        CQLMessageHandler.this.handleErrorAndRelease(
            new OverloadedException(
                "Server is in overloaded state. Cannot accept more requests at this point"),
            (Envelope.Header) this.header);
      } else if (!this.isCorrupt) {
        CQLMessageHandler.this.processRequest(this.assembleFrame());
      }
    }

    protected void abort() {
      if (!this.isCorrupt) {
        this.releaseBuffersAndCapacity();
      }
    }
  }

  interface ErrorHandler {
    void accept(Throwable var1);
  }

  interface MessageConsumer<M extends Message> {
    void accept(Channel var1, M var2, Dispatcher.FlushItemConverter var3);
  }
}
