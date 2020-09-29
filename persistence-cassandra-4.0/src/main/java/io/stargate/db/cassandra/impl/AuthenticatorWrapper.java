package io.stargate.db.cassandra.impl;

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

  public static class SaslNegotiatorWrapper implements Authenticator.SaslNegotiator {
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
    public AuthenticatedUser<?> getAuthenticatedUser() throws AuthenticationException {
      try {
        return new AuthenticatedUserWrapper(wrapped.getAuthenticatedUser());
      } catch (org.apache.cassandra.exceptions.AuthenticationException e) {
        throw Conversion.toExternal(e);
      }
    }
  }

  public static class AuthenticatedUserWrapper
      implements AuthenticatedUser<org.apache.cassandra.auth.AuthenticatedUser> {
    private final org.apache.cassandra.auth.AuthenticatedUser wrapped;

    AuthenticatedUserWrapper(org.apache.cassandra.auth.AuthenticatedUser wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public String getName() {
      return wrapped.getName();
    }

    @Override
    public org.apache.cassandra.auth.AuthenticatedUser getWrapped() {
      return wrapped;
    }
  }
}
