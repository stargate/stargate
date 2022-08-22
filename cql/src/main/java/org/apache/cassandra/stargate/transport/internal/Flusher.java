package org.apache.cassandra.stargate.transport.internal;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.cassandra.net.FrameEncoder.Payload;
import org.apache.cassandra.net.FrameEncoder.PayloadAllocator;
import org.apache.cassandra.stargate.transport.internal.Message.Response;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Flusher implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Flusher.class);

  @VisibleForTesting
  public static final int MAX_FRAMED_PAYLOAD_SIZE = Math.min(131072, 131072 - Math.max(10, 12));

  protected final EventLoop eventLoop;
  private final ConcurrentLinkedQueue<Flusher.FlushItem<?>> queued;
  protected final AtomicBoolean scheduled;
  protected final List<Flusher.FlushItem<?>> processed;
  private final HashSet<Channel> channels;
  private final Map<Channel, Flusher.FlushBuffer> payloads;

  static Flusher legacy(EventLoop loop) {
    return new Flusher.LegacyFlusher(loop);
  }

  static Flusher immediate(EventLoop loop) {
    return new Flusher.ImmediateFlusher(loop);
  }

  void start() {
    if (!this.scheduled.get() && this.scheduled.compareAndSet(false, true)) {
      this.eventLoop.execute(this);
    }
  }

  private Flusher(EventLoop eventLoop) {
    this.queued = new ConcurrentLinkedQueue();
    this.scheduled = new AtomicBoolean(false);
    this.processed = new ArrayList();
    this.channels = new HashSet();
    this.payloads = new HashMap();
    this.eventLoop = eventLoop;
  }

  void enqueue(Flusher.FlushItem<?> item) {
    this.queued.add(item);
  }

  Flusher.FlushItem<?> poll() {
    return (Flusher.FlushItem) this.queued.poll();
  }

  boolean isEmpty() {
    return this.queued.isEmpty();
  }

  private void processUnframedResponse(Flusher.FlushItem.Unframed flush) {
    flush.channel.write(flush.response, flush.channel.voidPromise());
    this.channels.add(flush.channel);
  }

  private void processFramedResponse(Flusher.FlushItem.Framed flush) {
    Envelope outbound = (Envelope) flush.response;
    if (CQLMessageHandler.envelopeSize(outbound.header) >= MAX_FRAMED_PAYLOAD_SIZE) {
      this.flushLargeMessage(flush.channel, outbound, flush.allocator);
    } else {
      ((Flusher.FlushBuffer)
              this.payloads.computeIfAbsent(
                  flush.channel,
                  (channel) -> {
                    return new Flusher.FlushBuffer(channel, flush.allocator, 5);
                  }))
          .add((Envelope) flush.response);
    }
  }

  private void flushLargeMessage(Channel channel, Envelope outbound, PayloadAllocator allocator) {
    ByteBuf body = outbound.body;
    boolean firstFrame = true;

    while (body.readableBytes() > 0 || firstFrame) {
      int payloadSize = Math.min(body.readableBytes(), MAX_FRAMED_PAYLOAD_SIZE);
      Payload payload = allocator.allocate(false, payloadSize);
      if (logger.isTraceEnabled()) {
        logger.trace(
            "Allocated initial buffer of {} for 1 large item",
            FBUtilities.prettyPrintMemory((long) payload.buffer.capacity()));
      }

      ByteBuffer buf = payload.buffer;
      if (payloadSize >= MAX_FRAMED_PAYLOAD_SIZE) {
        buf.limit(MAX_FRAMED_PAYLOAD_SIZE);
      }

      if (firstFrame) {
        outbound.encodeHeaderInto(buf);
        firstFrame = false;
      }

      int remaining = Math.min(buf.remaining(), body.readableBytes());
      if (remaining > 0) {
        buf.put(body.slice(body.readerIndex(), remaining).nioBuffer());
      }

      body.readerIndex(body.readerIndex() + remaining);
      this.writeAndFlush(channel, payload);
    }
  }

  private void writeAndFlush(Channel channel, Payload payload) {
    payload.finish();
    channel.writeAndFlush(payload, channel.voidPromise());
  }

  protected boolean processQueue() {
    boolean doneWork;
    Flusher.FlushItem flush;
    for (doneWork = false; (flush = this.poll()) != null; doneWork = true) {
      if (flush.kind == Flusher.FlushItem.Kind.FRAMED) {
        this.processFramedResponse((Flusher.FlushItem.Framed) flush);
      } else {
        this.processUnframedResponse((Flusher.FlushItem.Unframed) flush);
      }

      this.processed.add(flush);
    }

    return doneWork;
  }

  protected void flushWrittenChannels() {
    Iterator var1 = this.channels.iterator();

    while (var1.hasNext()) {
      Channel channel = (Channel) var1.next();
      channel.flush();
    }

    var1 = this.payloads.values().iterator();

    while (var1.hasNext()) {
      Flusher.FlushBuffer buffer = (Flusher.FlushBuffer) var1.next();
      buffer.finish();
    }

    var1 = this.processed.iterator();

    while (var1.hasNext()) {
      Flusher.FlushItem<?> item = (Flusher.FlushItem) var1.next();
      item.release();
    }

    this.payloads.clear();
    this.channels.clear();
    this.processed.clear();
  }

  private static final class ImmediateFlusher extends Flusher {
    private ImmediateFlusher(EventLoop eventLoop) {
      super(eventLoop);
    }

    public void run() {
      this.scheduled.set(false);

      try {
        this.processQueue();
      } finally {
        this.flushWrittenChannels();
      }
    }
  }

  private static final class LegacyFlusher extends Flusher {
    int runsSinceFlush;
    int runsWithNoWork;

    private LegacyFlusher(EventLoop eventLoop) {
      super(eventLoop);
      this.runsSinceFlush = 0;
      this.runsWithNoWork = 0;
    }

    public void run() {
      boolean doneWork = this.processQueue();
      ++this.runsSinceFlush;
      if (!doneWork || this.runsSinceFlush > 2 || this.processed.size() > 50) {
        this.flushWrittenChannels();
        this.runsSinceFlush = 0;
      }

      if (doneWork) {
        this.runsWithNoWork = 0;
      } else if (++this.runsWithNoWork > 5) {
        this.scheduled.set(false);
        if (this.isEmpty() || !this.scheduled.compareAndSet(false, true)) {
          return;
        }
      }

      this.eventLoop.schedule(this, 10000L, TimeUnit.NANOSECONDS);
    }
  }

  private class FlushBuffer extends ArrayList<Envelope> {
    private final Channel channel;
    private final PayloadAllocator allocator;
    private int sizeInBytes = 0;

    FlushBuffer(Channel channel, PayloadAllocator allocator, int initialCapacity) {
      super(initialCapacity);
      this.channel = channel;
      this.allocator = allocator;
    }

    public boolean add(Envelope toFlush) {
      this.sizeInBytes += CQLMessageHandler.envelopeSize(toFlush.header);
      return super.add(toFlush);
    }

    private Payload allocate(int requiredBytes, int maxItems) {
      int bufferSize = Math.min(requiredBytes, Flusher.MAX_FRAMED_PAYLOAD_SIZE);
      Payload payload = this.allocator.allocate(true, bufferSize);
      if (payload.remaining() >= Flusher.MAX_FRAMED_PAYLOAD_SIZE) {
        payload.buffer.limit(payload.buffer.position() + bufferSize);
      }

      if (Flusher.logger.isTraceEnabled()) {
        Flusher.logger.trace(
            "Allocated initial buffer of {} for up to {} items",
            FBUtilities.prettyPrintMemory((long) payload.buffer.capacity()),
            maxItems);
      }

      return payload;
    }

    public void finish() {
      int writtenBytes = 0;
      int messagesToWrite = this.size();
      Payload sending = this.allocate(this.sizeInBytes, messagesToWrite);

      for (Iterator var5 = this.iterator(); var5.hasNext(); --messagesToWrite) {
        Envelope f = (Envelope) var5.next();
        int messageSize = CQLMessageHandler.envelopeSize(f.header);
        if (sending.remaining() < messageSize) {
          Flusher.this.writeAndFlush(this.channel, sending);
          sending = this.allocate(this.sizeInBytes - writtenBytes, messagesToWrite);
        }

        f.encodeInto(sending.buffer);
        writtenBytes += messageSize;
      }

      Flusher.this.writeAndFlush(this.channel, sending);
    }
  }

  static class FlushItem<T> {
    final Flusher.FlushItem.Kind kind;
    final Channel channel;
    final T response;
    final Envelope request;
    final Consumer<Flusher.FlushItem<T>> tidy;

    FlushItem(
        Flusher.FlushItem.Kind kind,
        Channel channel,
        T response,
        Envelope request,
        Consumer<Flusher.FlushItem<T>> tidy) {
      this.kind = kind;
      this.channel = channel;
      this.request = request;
      this.response = response;
      this.tidy = tidy;
    }

    void release() {
      this.tidy.accept(this);
    }

    static class Unframed extends Flusher.FlushItem<Response> {
      Unframed(
          Channel channel,
          Response response,
          Envelope request,
          Consumer<Flusher.FlushItem<Response>> tidy) {
        super(Flusher.FlushItem.Kind.UNFRAMED, channel, response, request, tidy);
      }
    }

    static class Framed extends Flusher.FlushItem<Envelope> {
      final PayloadAllocator allocator;

      Framed(
          Channel channel,
          Envelope response,
          Envelope request,
          PayloadAllocator allocator,
          Consumer<Flusher.FlushItem<Envelope>> tidy) {
        super(Flusher.FlushItem.Kind.FRAMED, channel, response, request, tidy);
        this.allocator = allocator;
      }
    }

    static enum Kind {
      FRAMED,
      UNFRAMED;

      private Kind() {}
    }
  }
}
