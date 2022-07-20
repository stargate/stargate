package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(createSession = false)
public class SSLTest extends BaseIntegrationTest {

  private static final String CLIENT_TRUSTSTORE_PATH = "/client.truststore";

  private static final String CLIENT_TRUSTSTORE_PASSWORD = "fakePasswordForTests";
  private static final String CLIENT_KEYSTORE_PATH = "/client.keystore";
  private static final String CLIENT_KEYSTORE_PASSWORD = "fakePasswordForTests";

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.useSSLForCQL(true);
  }

  @Test
  @DisplayName("Should execute a simple query over SSL")
  public void simpleQuery(CqlSessionBuilder sessionBuilder) {
    CqlSession session = sessionBuilder.withSslContext(createSSLContext()).build();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM system.local");
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(1);
  }

  @Test
  @DisplayName("Should fail to connect without using an SSL context")
  public void noSSLContext(CqlSessionBuilder sessionBuilder) {
    assertThatThrownBy(sessionBuilder::build)
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining(
            "Could not reach any contact point, make sure you've provided valid addresses");
  }

  private SSLContext createSSLContext() {
    try {
      SSLContext context = SSLContext.getInstance("SSL");

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      try (InputStream tsf = getClass().getResourceAsStream(CLIENT_TRUSTSTORE_PATH)) {
        KeyStore ts = KeyStore.getInstance("JKS");
        char[] password = CLIENT_TRUSTSTORE_PASSWORD.toCharArray();
        ts.load(tsf, password);
        tmf.init(ts);
      }

      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      try (InputStream ksf = getClass().getResourceAsStream(CLIENT_KEYSTORE_PATH)) {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] password = CLIENT_KEYSTORE_PASSWORD.toCharArray();
        ks.load(ksf, password);
        kmf.init(ks, password);
      }

      context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
      return context;
    } catch (Exception e) {
      throw new AssertionError("Unexpected error while creating SSL context", e);
    }
  }
}
