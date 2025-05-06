package org.apache.cassandra.stargate.transport.internal;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.NoopCompressor;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.impl.MetricsImpl;
import io.stargate.db.Result;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.ResultMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The purpose of this test is to ensure that the Stargate CQL protocol codec is compatible with the
 * native protocol codec. This is especially important when adding flags to headers as the order of
 * encoding/decoding is significant. The tests uses <a
 * href="https://github.com/datastax/native-protocol"/>as a reference implementation for native
 * protocol.
 */
public class CQLProtocolCompatibilityTest {
  private static final UUID TRACING_ID = UUID.randomUUID();
  private static final Map<String, ByteBuffer> CUSTOM_PAYLOAD =
      Collections.singletonMap("test-key", ByteBuffer.wrap("test-value".getBytes()));
  private static final List<String> WARNINGS = ImmutableList.of("warning1");

  @BeforeAll
  public static void setup() {
    Metrics metrics = new MetricsImpl();
    ClientMetrics.instance.init(Collections.emptyList(), metrics.getMeterRegistry(), null, 0);
  }

  @Test
  public void encoderCompatibleWithNativeProtocolDecoder() {
    // Prepare a ResultMessage with custom payload, warnings and tracingId
    ResultMessage resultMessage = new MockingResultMessage(TRACING_ID, CUSTOM_PAYLOAD, WARNINGS);

    // Encode the ResultMessage via the Stargate CQL protocol encoder
    Envelope envelope = resultMessage.encode(ProtocolVersion.V4);
    List<Object> result = new ArrayList<>();
    Envelope.Encoder.instance.encode(null, envelope, result);

    ByteBuf header = (ByteBuf) result.get(0);
    ByteBuf body = (ByteBuf) result.get(1);

    int messageSize = header.readableBytes() + body.readableBytes();
    ByteBuf headerAndBody = CBUtil.allocator.buffer(messageSize);
    headerAndBody.writeBytes(header).writeBytes(body);

    // Decode the encoded message by the native protocol decoder
    Compressor<ByteBuf> compressor = new NoopCompressor<>();
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    PrimitiveCodec<ByteBuf> primitiveCodec = new ByteBufPrimitiveCodec(allocator);
    FrameCodec<ByteBuf> frameCodec = FrameCodec.defaultClient(primitiveCodec, compressor);

    com.datastax.oss.protocol.internal.Frame decodedFrame = frameCodec.decode(headerAndBody);
    assertThat(decodedFrame).isNotNull();
    assertThat(decodedFrame.tracingId).isEqualTo(TRACING_ID);
    assertThat(decodedFrame.customPayload).isEqualTo(CUSTOM_PAYLOAD);
    assertThat(decodedFrame.warnings).isEqualTo(WARNINGS);
    assertThat(decodedFrame.message)
        .isInstanceOf(com.datastax.oss.protocol.internal.response.result.Void.class);
  }

  @Test
  public void decoderCompatibleWithNativeProtocolEncoder() {
    // Prepare native protocol frame with custom payload, warnings and tracingId
    int streamId = 1;
    com.datastax.oss.protocol.internal.response.Result resultMessage =
        com.datastax.oss.protocol.internal.response.result.Void.INSTANCE;
    Compressor<ByteBuf> compressor = new NoopCompressor<>();
    UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    PrimitiveCodec<ByteBuf> primitiveCodec = new ByteBufPrimitiveCodec(allocator);

    com.datastax.oss.protocol.internal.Frame frame =
        com.datastax.oss.protocol.internal.Frame.forResponse(
            ProtocolConstants.Version.V4,
            streamId,
            TRACING_ID,
            CUSTOM_PAYLOAD,
            WARNINGS,
            resultMessage);

    int protocolV4 = ProtocolConstants.Version.V4; // customPayload is supported from V4
    FrameCodec<ByteBuf> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            registry ->
                registry.addEncoder(
                    new com.datastax.oss.protocol.internal.response.Result.Codec(protocolV4)));

    // Encode via native protocol encoder
    ByteBuf encodedFrame = frameCodec.encode(frame);

    // Decode the encoded frame via the Stargate CQL protocol decoder
    Envelope.Decoder decoder = new Envelope.Decoder(v -> v >= 4);
    List<Object> results = new ArrayList<>();
    decoder.decode(null, encodedFrame, results);
    Envelope decodedEnvelope = (Envelope) results.get(0);
    ResultMessage decodedResult =
        (ResultMessage) Message.responseDecoder().decode(null, decodedEnvelope);

    assertThat(decodedResult).isNotNull();
    assertThat(decodedResult.tracingId).isEqualTo(TRACING_ID);
    assertThat(decodedResult.customPayload).isEqualTo(CUSTOM_PAYLOAD);
    assertThat(decodedResult.warnings).isEqualTo(WARNINGS);
    assertThat(decodedResult.type).isEqualTo(Message.Type.RESULT);
  }

  public static class MockingResultMessage extends ResultMessage {
    protected MockingResultMessage(
        UUID tracingId, Map<String, ByteBuffer> customPayload, List<String> warnings) {
      super(new Result.Void());
      this.tracingId = tracingId;
      this.customPayload = customPayload;
      this.warnings = warnings;
    }
  }
}
