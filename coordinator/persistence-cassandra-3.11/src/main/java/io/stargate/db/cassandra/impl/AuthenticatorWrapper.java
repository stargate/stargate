/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    return new SaslNegotiatorWrapper(wrapped.newSaslNegotiator(clientAddress));
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
