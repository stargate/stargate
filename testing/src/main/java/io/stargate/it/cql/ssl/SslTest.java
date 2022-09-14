package io.stargate.it.cql.ssl;

import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateSpec;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base test class for SSL tests. */
@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(createSession = false)
public class SslTest extends BaseIntegrationTest {

  protected static final String CLIENT_TRUSTSTORE_PATH = "/client.truststore";

  protected static final String CLIENT_TRUSTSTORE_PASSWORD = "fakePasswordForTests";

  protected static final String CLIENT_KEYSTORE_PATH = "/client.keystore";

  protected static final String CLIENT_KEYSTORE_PASSWORD = "fakePasswordForTests";

  protected static SSLContext createSSLContext() {
    try {
      SSLContext context = SSLContext.getInstance("SSL");

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      try (InputStream tsf = SslTest.class.getResourceAsStream(CLIENT_TRUSTSTORE_PATH)) {
        KeyStore ts = KeyStore.getInstance("JKS");
        char[] password = CLIENT_TRUSTSTORE_PASSWORD.toCharArray();
        ts.load(tsf, password);
        tmf.init(ts);
      }

      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      try (InputStream ksf = SslTest.class.getResourceAsStream(CLIENT_KEYSTORE_PATH)) {
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
