/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.auth;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import io.stargate.db.Authenticator.SaslNegotiator;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;

public abstract class PlainTextTokenSaslNegotiator implements SaslNegotiator {
  static final byte NUL = 0;

  protected final AuthenticationService authentication;
  private final Authenticator.SaslNegotiator wrapped;
  protected AuthenticationSubject authenticationSubject;
  protected final String tokenUsername;
  protected final int tokenMaxLength;

  public PlainTextTokenSaslNegotiator(
      AuthenticationService authentication,
      SaslNegotiator wrapped,
      String tokenUsername,
      int tokenMaxLength) {
    this.authentication = authentication;
    this.wrapped = wrapped;
    this.tokenUsername = tokenUsername;
    this.tokenMaxLength = tokenMaxLength;
  }

  @Override
  public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
    if (attemptTokenAuthentication(clientResponse)) {
      return null;
    }
    return wrapped.evaluateResponse(clientResponse);
  }

  @Override
  public boolean isComplete() {
    return authenticationSubject != null || wrapped.isComplete();
  }

  @Override
  public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
    if (authenticationSubject != null) {
      return AuthenticatedUser.of(
          authenticationSubject.roleName(),
          authenticationSubject.token(),
          authenticationSubject.isFromExternalAuth(),
          authenticationSubject.customProperties());
    } else {
      return wrapped.getAuthenticatedUser();
    }
  }

  public abstract boolean attemptTokenAuthentication(byte[] clientResponse);

  /**
   * Copy of the private method:
   * org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator#decodeCredentials(byte[]).
   *
   * @param bytes encoded credentials string sent by the client
   * @return a pair contain the username and password
   * @throws AuthenticationException if either the authnId or password is null
   */
  public static Credentials decodeCredentials(byte[] bytes) throws AuthenticationException {
    byte[] user = null;
    byte[] pass = null;
    int end = bytes.length;
    for (int i = bytes.length - 1; i >= 0; i--) {
      if (bytes[i] == NUL) {
        if (pass == null) {
          pass = Arrays.copyOfRange(bytes, i + 1, end);
        } else if (user == null) {
          user = Arrays.copyOfRange(bytes, i + 1, end);
        } else {
          throw new AuthenticationException(
              "Credential format error: username or password is empty or contains NUL(\\0) character");
        }

        end = i;
      }
    }

    if (pass == null || pass.length == 0) {
      throw new AuthenticationException("Password must not be null");
    }
    if (user == null || user.length == 0) {
      throw new AuthenticationException("Authentication ID must not be null");
    }

    String passwd = new String(pass, StandardCharsets.UTF_8);
    return new Credentials(new String(user, StandardCharsets.UTF_8), passwd.toCharArray());
  }
}
