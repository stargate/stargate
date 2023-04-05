package io.stargate.db.dse.impl;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import java.net.InetAddress;
import javax.security.cert.X509Certificate;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;

public class AuthenticatorWrapper implements Authenticator {
  private final IAuthenticator wrapped;

  public AuthenticatorWrapper(IAuthenticator wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public String getInternalClassName() {
    return wrapped.getClass().getName();
  }

  @Override
  public boolean requireAuthentication() {
    return wrapped.requireAuthentication();
  }

  @Override
  public SaslNegotiator newSaslNegotiator(
      InetAddress clientAddress, X509Certificate[] certificates) {
    return new SaslNegotiatorWrapper(wrapped.newSaslNegotiator(clientAddress, certificates));
  }

  public static class SaslNegotiatorWrapper implements SaslNegotiator {
    private final IAuthenticator.SaslNegotiator wrapped;

    SaslNegotiatorWrapper(IAuthenticator.SaslNegotiator wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
      try {
        return wrapped.evaluateResponse(clientResponse);
      } catch (org.apache.cassandra.exceptions.AuthenticationException e) {
        throw Conversion.toExternal(e);
      }
    }

    @Override
    public boolean isComplete() {
      return wrapped.isComplete();
    }

    @Override
    public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
      try {
        return AuthenticatedUser.of(wrapped.getAuthenticatedUser().getName());
      } catch (org.apache.cassandra.exceptions.AuthenticationException e) {
        throw Conversion.toExternal(e);
      }
    }
  }
}
