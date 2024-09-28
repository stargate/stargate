package org.apache.cassandra.stargate.transport.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.NoopCompressor;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.stargate.db.Result;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.internal.messages.ResultMessage;
import org.mockito.Mockito;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The purpose of this test is to ensure that the Stargate CQL protocol codec is compatible with the native
 * protocol codec. This is especially important when adding flags to headers as the order of encoding/decoding is significant.
 * The tests uses <a href="https://github.com/datastax/native-protocol"/> as a reference implementation for native protocol.
 */
public class CQLProtocolCompatibilityTest
{
    private static final UUID TRACING_ID = UUID.randomUUID();
    private static final Map<String, ByteBuffer> CUSTOM_PAYLOAD = Collections.singletonMap("test-key", ByteBuffer.wrap("test-value".getBytes()));
    private static final List<String> WARNINGS = ImmutableList.of("warning1");

    private EmbeddedChannel channel;

    @BeforeAll
    public static void initClientMetrics() {
        ClientInfoMetricsTagProvider clientTagProvider = mock(ClientInfoMetricsTagProvider.class);
        CqlServer server1 = mock(CqlServer.class);
        List<CqlServer> servers = Arrays.asList(server1);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ClientMetrics.instance.init(servers, meterRegistry, clientTagProvider, 0d);
    }

    @BeforeEach
    public void initChannel() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    public void closeChannel() {
        channel.close();
    }

    @Test
    public void encoderCompatibleWithNativeProtocolDecoder()
    {
        // Prepare a ResultMessage with custom payload, warnings and tracingId
        ResultMessage resultMessage = new MockingResultMessage(TRACING_ID, CUSTOM_PAYLOAD, WARNINGS);

        channel.pipeline().addLast(new Frame.Encoder());
        channel.pipeline().addLast(new Frame.OutboundBodyTransformer());
        channel.pipeline().addLast(new Message.ProtocolEncoder());

        // Encode the ResultMessage via the Stargate CQL protocol encoder
        channel.writeOutbound(resultMessage);
        channel.finish();

        ByteBuf header = channel.readOutbound();
        ByteBuf body = channel.readOutbound();

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
        assertThat(decodedFrame.message).isInstanceOf(Void.class);
    }

    @Test
    public void decoderCompatibleWithNativeProtocolEncoder()
    {
        // Prepare native protocol frame with custom payload, warnings and tracingId
        int streamId = 1;
        com.datastax.oss.protocol.internal.response.Result resultMessage = com.datastax.oss.protocol.internal.response.result.Void.INSTANCE;
        Compressor<ByteBuf> compressor = new NoopCompressor<>();
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
        PrimitiveCodec<ByteBuf> primitiveCodec = new ByteBufPrimitiveCodec(allocator);

        com.datastax.oss.protocol.internal.Frame frame = com.datastax.oss.protocol.internal.Frame.forResponse(
            ProtocolConstants.Version.V4,
            streamId,
            TRACING_ID,
            CUSTOM_PAYLOAD,
            WARNINGS,
            resultMessage);

        int protocolV4 = ProtocolConstants.Version.V4; // customPayload is supported from V4
        FrameCodec<ByteBuf> frameCodec = new FrameCodec<>(
            primitiveCodec, compressor,
            registry -> registry.addEncoder(new com.datastax.oss.protocol.internal.response.Result.Codec(protocolV4)));

        // Encode via native protocol encoder
        ByteBuf encodedFrame = frameCodec.encode(frame);

        // Write the encoded frame to the channel, that will invoke the Stargate CQL protocol decoder
        EmbeddedChannel channel = new EmbeddedChannel();
        Connection.Factory connectionFactory = Mockito.mock(Connection.Factory.class);
        channel.pipeline().addLast(new Frame.Decoder(connectionFactory));
        channel.pipeline().addLast(new Frame.InboundBodyTransformer());
        channel.pipeline().addLast(new Message.ProtocolDecoder());

        channel.writeInbound(encodedFrame);
        channel.finish();

        ResultMessage decodedResult = channel.readInbound();

        assertThat(decodedResult).isNotNull();
        assertThat(decodedResult.tracingId).isEqualTo(TRACING_ID);
        assertThat(decodedResult.customPayload).isEqualTo(CUSTOM_PAYLOAD);
        assertThat(decodedResult.warnings).isEqualTo(WARNINGS);
        assertThat(decodedResult.type).isEqualTo(Message.Type.RESULT);
    }

    public static class MockingResultMessage extends ResultMessage
    {
        protected MockingResultMessage(UUID tracingId, Map<String, ByteBuffer> customPayload, List<String> warnings)
        {
            super(new Result.Void());
            this.tracingId = tracingId;
            this.customPayload = customPayload;
            this.warnings = warnings;
        }
    }
}


