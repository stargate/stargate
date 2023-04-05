package io.stargate.db.cassandra.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class TokenGenerationTest extends BaseCassandraTest {
  @Test
  public void testSingleAddressReturnsTheSameTokens() throws UnknownHostException {
    final InetAddress inetAddress = InetAddress.getByName("172.68.13.2");
    final int numTokens = 8;
    Set<String> tokens1 = StargateSystemKeyspace.generateRandomTokens(inetAddress, numTokens);
    Set<String> tokens2 = StargateSystemKeyspace.generateRandomTokens(inetAddress, numTokens);
    assertThat(tokens1).hasSize(numTokens);
    assertThat(tokens1).isEqualTo(tokens2);
  }

  @Test
  public void testTwoAddressesReturnsDifferentTokens() throws UnknownHostException {
    final InetAddress inetAddress1 = InetAddress.getByName("172.68.13.2");
    final InetAddress inetAddress2 = InetAddress.getByName("172.68.14.3");
    final int numTokens = 8;
    Set<String> tokens1 = StargateSystemKeyspace.generateRandomTokens(inetAddress1, numTokens);
    Set<String> tokens2 = StargateSystemKeyspace.generateRandomTokens(inetAddress2, numTokens);
    assertThat(tokens1).hasSize(numTokens);
    assertThat(tokens2).hasSize(numTokens);
    assertThat(tokens1).isNotEqualTo(tokens2);
  }
}
