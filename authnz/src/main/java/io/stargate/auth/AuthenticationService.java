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
package io.stargate.auth;

import io.stargate.db.Authenticator.SaslNegotiator;
import io.stargate.db.ClientInfo;
import java.util.Map;

public interface AuthenticationService {

  String createToken(String key, String secret, Map<String, String> headers)
      throws UnauthorizedException;

  String createToken(String key, Map<String, String> headers) throws UnauthorizedException;

  AuthenticationSubject validateToken(String token) throws UnauthorizedException;

  default AuthenticationSubject validateToken(String token, Map<String, String> headers)
      throws UnauthorizedException {
    return validateToken(token);
  }

  default AuthenticationSubject validateToken(String token, ClientInfo clientInfo)
      throws UnauthorizedException {
    return validateToken(token);
  }

  SaslNegotiator getSaslNegotiator(SaslNegotiator wrapped, ClientInfo clientInfo);
}
