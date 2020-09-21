package org.apache.cassandra.stargate.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.transport.internal.PlainTextTokenSaslNegotiator.Credentials;
import org.junit.Test;

public class PlainTextTokenSaslNegotiatorTest {
  @Test
  public void decodeCredentials() {
    Credentials credentials =
        PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 97, 98, 99, 0, 97, 98, 99});
    assertThat(credentials.username).isEqualTo("abc");
    assertThat(credentials.password).isEqualTo("abc");
  }

  @Test
  public void invalidDecodeCredentials() {
    try { // Empty
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e).hasMessage("Password must not be null");
    }

    try { // Empty authzid
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e).hasMessage("Password must not be null");
    }

    try { // Empty authzid and authnid
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 0});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e).hasMessage("Password must not be null");
    }

    try { // Only authzid
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {97, 98, 99, 0, 0});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e).hasMessage("Password must not be null");
    }

    try { // Empty authzid, but valid authnid and password
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 97, 0, 97, 0});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e)
          .hasMessage(
              "Credential format error: username or password is empty or contains NUL(\\0) character");
    }

    try { // Non-empty authzid
      PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {97, 0, 97, 0, 97, 0});
      fail("Expected AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e)
          .hasMessage(
              "Credential format error: username or password is empty or contains NUL(\\0) character");
    }
  }

  @Test
  public void tokenAuthentication() {}
}
