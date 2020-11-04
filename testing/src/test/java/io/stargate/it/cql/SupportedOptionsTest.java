package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolV4ClientCodecs;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.Supported;
import io.netty.buffer.ByteBuf;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionOptions;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SupportedOptionsTest extends BaseOsgiIntegrationTest {
  /** Custom session builder that records supported options. */
  private static class RecordingSupportedOptionsSessionBuilder
      extends SessionBuilder<RecordingSupportedOptionsSessionBuilder, CqlSession> {
    private final List<Map<String, List<String>>> recordedOptions = new ArrayList<>();

    public List<Map<String, List<String>>> getRecordedOptions() {
      // Make a copy in case it's modified concurrently
      synchronized (recordedOptions) {
        return new ArrayList<>(recordedOptions);
      }
    }

    private class RecordingSupportedOptionsCodec extends Supported.Codec {
      public RecordingSupportedOptionsCodec(int protocolVersion) {
        super(protocolVersion);
      }

      @Override
      public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
        Supported supported = (Supported) super.decode(source, decoder);
        synchronized (recordedOptions) {
          recordedOptions.add(supported.options);
        }
        return supported;
      }
    }

    private class RecordingDriverContext extends DefaultDriverContext {
      public RecordingDriverContext(
          DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
        super(configLoader, programmaticArguments);
      }

      @Override
      protected FrameCodec<ByteBuf> buildFrameCodec() {
        return new FrameCodec<>(
            getPrimitiveCodec(),
            getCompressor(),
            new ProtocolV4ClientCodecs() {
              @Override
              public void registerCodecs(Registry registry) {
                // This has to override all codecs because attempting to replace an existing codec
                // results in an exception.
                registry
                    .addEncoder(new AuthResponse.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Batch.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Execute.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Options.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Prepare.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Query.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Register.Codec(ProtocolConstants.Version.V4))
                    .addEncoder(new Startup.Codec(ProtocolConstants.Version.V4));

                registry
                    .addDecoder(new AuthChallenge.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new Authenticate.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new AuthSuccess.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new Error.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new Event.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new Ready.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(new Result.Codec(ProtocolConstants.Version.V4))
                    .addDecoder(
                        new RecordingSupportedOptionsCodec(
                            ProtocolConstants.Version.V4)); // Overridden
              }
            });
      }
    }

    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      return new RecordingDriverContext(configLoader, programmaticArguments);
    }

    @Override
    protected CqlSession wrap(@NotNull CqlSession defaultSession) {
      return defaultSession;
    }
  }

  @Test
  @DisplayName("Should contain expected supported options")
  public void supportedOptionsTest(StargateEnvironmentInfo stargate) {
    RecordingSupportedOptionsSessionBuilder builder = new RecordingSupportedOptionsSessionBuilder();
    OptionsMap driverOptions = CqlSessionOptions.defaultConfig();
    // Force protocol V4 because that's the only codec that's overridden
    driverOptions.put(TypedDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString());
    builder.withConfigLoader(DriverConfigLoader.fromMap(driverOptions));
    for (StargateConnectionInfo node : stargate.nodes()) {
      builder.addContactPoint(new InetSocketAddress(node.seedAddress(), node.cqlPort()));
    }

    // This only validates the presence of options keys because validating the value depends
    // on the specific backend and could make this test brittle.
    try (CqlSession notUsed = builder.build()) {
      List<Map<String, List<String>>> recordedOptions = builder.getRecordedOptions();
      assertThat(recordedOptions).isNotEmpty();
      for (Map<String, List<String>> options : recordedOptions) {
        assertThat(options).containsKey("CQL_VERSION");
        assertThat(options).containsKey("PROTOCOL_VERSIONS");
        assertThat(options).containsKey("COMPRESSION");
        if (backend.isDse()) {
          assertThat(options).containsKey("PAGE_UNIT");
          assertThat(options).containsKey("SERVER_VERSION");
          assertThat(options).containsKey("PRODUCT_TYPE");
          assertThat(options).containsKey("EMULATE_DBAAS_DEFAULTS");
        }
      }
    }
  }
}
