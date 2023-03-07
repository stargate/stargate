package io.stargate.it.cql.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.storage.SslForCqlParameters;
import io.stargate.it.storage.StargateParameters;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

public class BasicSslTest extends SslTest {

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.sslForCqlParameters(SslForCqlParameters.builder().enabled(true).build());
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
}
