package org.apache.cassandra.stargate.transport.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.metrics.ClientMessageSizeMetrics;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Envelope {
  public static final byte PROTOCOL_VERSION_MASK = 127;
  public final Envelope.Header header;
  public final ByteBuf body;

  public Envelope(Envelope.Header header, ByteBuf body) {
    this.header = header;
    this.body = body;
  }

  public void retain() {
    this.body.retain();
  }

  public boolean release() {
    return this.body.release();
  }

  @VisibleForTesting
  public Envelope clone() {
    return new Envelope(
        this.header, Unpooled.wrappedBuffer(ByteBufferUtil.clone(this.body.nioBuffer())));
  }

  public static Envelope create(
      Message.Type type,
      int streamId,
      ProtocolVersion version,
      EnumSet<Envelope.Header.Flag> flags,
      ByteBuf body) {
    Envelope.Header header =
        new Envelope.Header(version, flags, streamId, type, (long) body.readableBytes());
    return new Envelope(header, body);
  }

  public ByteBuf encodeHeader() {
    ByteBuf buf = CBUtil.allocator.buffer(9);
    Message.Type type = this.header.type;
    buf.writeByte(type.direction.addToVersion(this.header.version.asInt()));
    buf.writeByte(Envelope.Header.Flag.serialize(this.header.flags));
    if (this.header.version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
      buf.writeShort(this.header.streamId);
    } else {
      buf.writeByte(this.header.streamId);
    }

    buf.writeByte(type.opcode);
    buf.writeInt(this.body.readableBytes());
    return buf;
  }

  public void encodeHeaderInto(ByteBuffer buf) {
    buf.put((byte) this.header.type.direction.addToVersion(this.header.version.asInt()));
    buf.put((byte) Envelope.Header.Flag.serialize(this.header.flags));
    if (this.header.version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
      buf.putShort((short) this.header.streamId);
    } else {
      buf.put((byte) this.header.streamId);
    }

    buf.put((byte) this.header.type.opcode);
    buf.putInt(this.body.readableBytes());
  }

  public void encodeInto(ByteBuffer buf) {
    this.encodeHeaderInto(buf);
    buf.put(this.body.nioBuffer());
  }

  public Envelope with(ByteBuf newBody) {
    return new Envelope(this.header, newBody);
  }

  private static long discard(ByteBuf buffer, long remainingToDiscard) {
    int availableToDiscard = (int) Math.min(remainingToDiscard, (long) buffer.readableBytes());
    buffer.skipBytes(availableToDiscard);
    return remainingToDiscard - (long) availableToDiscard;
  }

  @Sharable
  public static class Compressor extends MessageToMessageEncoder<Envelope> {
    public static Envelope.Compressor instance = new Envelope.Compressor();

    private Compressor() {}

    public void encode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        throws IOException {
      Connection connection = (Connection) ctx.channel().attr(Connection.attributeKey).get();
      if (source.header.type != Message.Type.STARTUP && connection != null) {
        org.apache.cassandra.stargate.transport.internal.Compressor compressor =
            connection.getCompressor();
        if (compressor == null) {
          results.add(source);
        } else {
          source.header.flags.add(Envelope.Header.Flag.COMPRESSED);
          results.add(compressor.compress(source));
        }
      } else {
        results.add(source);
      }
    }
  }

  @Sharable
  public static class Decompressor extends MessageToMessageDecoder<Envelope> {
    public static Envelope.Decompressor instance = new Envelope.Decompressor();

    private Decompressor() {}

    public void decode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        throws IOException {
      Connection connection = (Connection) ctx.channel().attr(Connection.attributeKey).get();
      if (source.header.flags.contains(Envelope.Header.Flag.COMPRESSED) && connection != null) {
        org.apache.cassandra.stargate.transport.internal.Compressor compressor =
            connection.getCompressor();
        if (compressor == null) {
          results.add(source);
        } else {
          results.add(compressor.decompress(source));
        }
      } else {
        results.add(source);
      }
    }
  }

  @Sharable
  public static class Encoder extends MessageToMessageEncoder<Envelope> {
    public static final Envelope.Encoder instance = new Envelope.Encoder();

    private Encoder() {}

    public void encode(ChannelHandlerContext ctx, Envelope source, List<Object> results) {
      ByteBuf serializedHeader = source.encodeHeader();
      int messageSize = serializedHeader.readableBytes() + source.body.readableBytes();
      ClientMessageSizeMetrics.bytesSent.inc((long) messageSize);
      ClientMessageSizeMetrics.bytesSentPerResponse.update(messageSize);
      results.add(serializedHeader);
      results.add(source.body);
    }
  }

  public static class Decoder extends ByteToMessageDecoder {
    private static final int MAX_TOTAL_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();
    private boolean discardingTooLongMessage;
    private long tooLongTotalLength;
    private long bytesToDiscard;
    private int tooLongStreamId;

    public Decoder() {}

    Envelope.Decoder.HeaderExtractionResult extractHeader(ByteBuffer buffer) {
      Preconditions.checkArgument(
          buffer.remaining() >= 9,
          "Undersized buffer supplied. Expected %s, actual %s",
          9,
          buffer.remaining());
      int idx = buffer.position();
      int firstByte = buffer.get(idx++);
      int versionNum = firstByte & 127;
      int flags = buffer.get(idx++);
      int streamId = buffer.getShort(idx);
      idx += 2;
      int opcode = buffer.get(idx++);
      long bodyLength = (long) buffer.getInt(idx);
      if (bodyLength < 0L) {
        return new Envelope.Decoder.HeaderExtractionResult.Error(
            new ProtocolException(
                "Invalid value for envelope header body length field: " + bodyLength),
            streamId,
            bodyLength);
      } else {
        Message.Direction direction = Message.Direction.extractFromVersion(firstByte);

        try {
          ProtocolVersion version =
              ProtocolVersion.decode(
                  versionNum, DatabaseDescriptor.getNativeTransportAllowOlderProtocols());
          EnumSet<Envelope.Header.Flag> decodedFlags = this.decodeFlags(version, flags);
          Message.Type type = Message.Type.fromOpcode(opcode, direction);
          return new Envelope.Decoder.HeaderExtractionResult.Success(
              new Envelope.Header(version, decodedFlags, streamId, type, bodyLength));
        } catch (ProtocolException var15) {
          return new Envelope.Decoder.HeaderExtractionResult.Error(var15, streamId, bodyLength);
        }
      }
    }

    @VisibleForTesting
    Envelope decode(ByteBuf buffer) {
      if (this.discardingTooLongMessage) {
        this.bytesToDiscard = Envelope.discard(buffer, this.bytesToDiscard);
        if (this.bytesToDiscard <= 0L) {
          this.fail();
        }

        return null;
      } else {
        int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
          return null;
        } else {
          int idx = buffer.readerIndex();
          int firstByte = buffer.getByte(idx++);
          Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
          int versionNum = firstByte & 127;
          ProtocolVersion version =
              ProtocolVersion.decode(
                  versionNum, DatabaseDescriptor.getNativeTransportAllowOlderProtocols());
          if (readableBytes < 9) {
            return null;
          } else {
            int flags = buffer.getByte(idx++);
            EnumSet<Envelope.Header.Flag> decodedFlags = this.decodeFlags(version, flags);
            int streamId = buffer.getShort(idx);
            idx += 2;

            Message.Type type;
            try {
              type = Message.Type.fromOpcode(buffer.getByte(idx++), direction);
            } catch (ProtocolException var17) {
              throw ErrorMessage.wrap(var17, streamId);
            }

            long bodyLength = buffer.getUnsignedInt(idx);
            idx += 4;
            long totalLength = bodyLength + 9L;
            if (totalLength > (long) MAX_TOTAL_LENGTH) {
              this.discardingTooLongMessage = true;
              this.tooLongStreamId = streamId;
              this.tooLongTotalLength = totalLength;
              this.bytesToDiscard = Envelope.discard(buffer, totalLength);
              if (this.bytesToDiscard <= 0L) {
                this.fail();
              }

              return null;
            } else if ((long) buffer.readableBytes() < totalLength) {
              return null;
            } else {
              ClientMessageSizeMetrics.bytesReceived.inc(totalLength);
              ClientMessageSizeMetrics.bytesReceivedPerRequest.update(totalLength);
              ByteBuf body = buffer.slice(idx, (int) bodyLength);
              body.retain();
              idx = (int) ((long) idx + bodyLength);
              buffer.readerIndex(idx);
              return new Envelope(
                  new Envelope.Header(version, decodedFlags, streamId, type, bodyLength), body);
            }
          }
        }
      }
    }

    private EnumSet<Envelope.Header.Flag> decodeFlags(ProtocolVersion version, int flags) {
      EnumSet<Envelope.Header.Flag> decodedFlags = Envelope.Header.Flag.deserialize(flags);
      if (version.isBeta() && !decodedFlags.contains(Envelope.Header.Flag.USE_BETA)) {
        throw new ProtocolException(
            String.format(
                "Beta version of the protocol used (%s), but USE_BETA flag is unset", version),
            version);
      } else {
        return decodedFlags;
      }
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results) {
      Envelope envelope = this.decode(buffer);
      if (envelope != null) {
        results.add(envelope);
      }
    }

    private void fail() {
      long tooLongTotalLength = this.tooLongTotalLength;
      this.tooLongTotalLength = 0L;
      this.discardingTooLongMessage = false;
      String msg =
          String.format(
              "Request is too big: length %d exceeds maximum allowed length %d.",
              tooLongTotalLength, MAX_TOTAL_LENGTH);
      throw ErrorMessage.wrap(new InvalidRequestException(msg), this.tooLongStreamId);
    }

    public abstract static class HeaderExtractionResult {
      private final Envelope.Decoder.HeaderExtractionResult.Outcome outcome;
      private final int streamId;
      private final long bodyLength;

      private HeaderExtractionResult(
          Envelope.Decoder.HeaderExtractionResult.Outcome outcome, int streamId, long bodyLength) {
        this.outcome = outcome;
        this.streamId = streamId;
        this.bodyLength = bodyLength;
      }

      boolean isSuccess() {
        return this.outcome == Envelope.Decoder.HeaderExtractionResult.Outcome.SUCCESS;
      }

      int streamId() {
        return this.streamId;
      }

      long bodyLength() {
        return this.bodyLength;
      }

      Envelope.Header header() {
        throw new IllegalStateException(
            String.format("Unable to provide header from extraction result : %s", this.outcome));
      }

      ProtocolException error() {
        throw new IllegalStateException(
            String.format("Unable to provide error from extraction result : %s", this.outcome));
      }

      private static class Error extends Envelope.Decoder.HeaderExtractionResult {
        private final ProtocolException error;

        private Error(ProtocolException error, int streamId, long bodyLength) {
          super(Envelope.Decoder.HeaderExtractionResult.Outcome.ERROR, streamId, bodyLength);
          this.error = error;
        }

        ProtocolException error() {
          return this.error;
        }
      }

      private static class Success extends Envelope.Decoder.HeaderExtractionResult {
        private final Envelope.Header header;

        Success(Envelope.Header header) {
          super(
              Envelope.Decoder.HeaderExtractionResult.Outcome.SUCCESS,
              header.streamId,
              header.bodySizeInBytes);
          this.header = header;
        }

        Envelope.Header header() {
          return this.header;
        }
      }

      static enum Outcome {
        SUCCESS,
        ERROR;

        private Outcome() {}
      }
    }
  }

  public static class Header {
    public static final int LENGTH = 9;
    public static final int BODY_LENGTH_SIZE = 4;
    public final ProtocolVersion version;
    public final EnumSet<Envelope.Header.Flag> flags;
    public final int streamId;
    public final Message.Type type;
    public final long bodySizeInBytes;

    private Header(
        ProtocolVersion version,
        EnumSet<Envelope.Header.Flag> flags,
        int streamId,
        Message.Type type,
        long bodySizeInBytes) {
      this.version = version;
      this.flags = flags;
      this.streamId = streamId;
      this.type = type;
      this.bodySizeInBytes = bodySizeInBytes;
    }

    public static enum Flag {
      COMPRESSED,
      TRACING,
      CUSTOM_PAYLOAD,
      WARNING,
      USE_BETA;

      private static final Envelope.Header.Flag[] ALL_VALUES = values();

      private Flag() {}

      public static EnumSet<Envelope.Header.Flag> deserialize(int flags) {
        EnumSet<Envelope.Header.Flag> set = EnumSet.noneOf(Envelope.Header.Flag.class);

        for (int n = 0; n < ALL_VALUES.length; ++n) {
          if ((flags & 1 << n) != 0) {
            set.add(ALL_VALUES[n]);
          }
        }

        return set;
      }

      public static int serialize(EnumSet<Envelope.Header.Flag> flags) {
        int i = 0;

        Envelope.Header.Flag flag;
        for (Iterator var2 = flags.iterator(); var2.hasNext(); i |= 1 << flag.ordinal()) {
          flag = (Envelope.Header.Flag) var2.next();
        }

        return i;
      }
    }
  }
}
