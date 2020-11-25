package io.stargate.db.cassandra.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.UnknownHostException;
import java.util.Set;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.junit.jupiter.api.Test;

public class TokenGenerationTest extends BaseCassandraTest {
  @Test
  public void testSingleAddressReturnsTheSameTokens() throws UnknownHostException {
    final InetAddressAndPort inetAddress =
        InetAddressAndPort.getByNameOverrideDefaults("172.68.13.2", 9042);
    final int numTokens = 8;
    Set<String> tokens1 = StargateSystemKeyspace.generateRandomTokens(inetAddress, numTokens);
    Set<String> tokens2 = StargateSystemKeyspace.generateRandomTokens(inetAddress, numTokens);
    assertThat(tokens1).hasSize(numTokens);
    assertThat(tokens1).isEqualTo(tokens2);
  }

  @Test
  public void testTwoAddressesReturnsDifferentTokens() throws UnknownHostException {
    final InetAddressAndPort inetAddress1 =
        InetAddressAndPort.getByNameOverrideDefaults("172.68.13.2", 9042);
    final InetAddressAndPort inetAddress2 =
        InetAddressAndPort.getByNameOverrideDefaults("172.68.14.3", 9042);
    final int numTokens = 8;
    Set<String> tokens1 = StargateSystemKeyspace.generateRandomTokens(inetAddress1, numTokens);
    Set<String> tokens2 = StargateSystemKeyspace.generateRandomTokens(inetAddress2, numTokens);
    assertThat(tokens1).hasSize(numTokens);
    assertThat(tokens2).hasSize(numTokens);
    assertThat(tokens1).isNotEqualTo(tokens2);
  }

  @Test
  public void testTwoAddressesPortsReturnsDifferentTokens() throws UnknownHostException {
    final String ipAddress = "172.68.13.2";
    final InetAddressAndPort inetAddress1 =
        InetAddressAndPort.getByNameOverrideDefaults(ipAddress, 9042);
    final InetAddressAndPort inetAddress2 =
        InetAddressAndPort.getByNameOverrideDefaults(ipAddress, 9043);
    final int numTokens = 8;
    Set<String> tokens1 = StargateSystemKeyspace.generateRandomTokens(inetAddress1, numTokens);
    Set<String> tokens2 = StargateSystemKeyspace.generateRandomTokens(inetAddress2, numTokens);
    assertThat(tokens1).hasSize(numTokens);
    assertThat(tokens2).hasSize(numTokens);
    assertThat(tokens1).isNotEqualTo(tokens2);
  }
}
