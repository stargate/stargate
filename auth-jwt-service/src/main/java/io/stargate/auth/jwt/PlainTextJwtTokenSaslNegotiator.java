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
package io.stargate.auth.jwt;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.Credentials;
import io.stargate.auth.PlainTextTokenSaslNegotiator;
import io.stargate.db.Authenticator.SaslNegotiator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainTextJwtTokenSaslNegotiator extends PlainTextTokenSaslNegotiator {

  private static final Logger logger =
      LoggerFactory.getLogger(PlainTextJwtTokenSaslNegotiator.class);
  private final Map<String, String> headers;

  public PlainTextJwtTokenSaslNegotiator(
      AuthenticationService authentication,
      SaslNegotiator wrapped,
      String tokenUsername,
      int tokenMaxLength,
      Map<String, String> headers) {
    super(authentication, wrapped, tokenUsername, tokenMaxLength);
    this.headers = headers;
  }

  @Override
  public boolean attemptTokenAuthentication(byte[] clientResponse) {
    try {
      Credentials credentials = decodeCredentials(clientResponse);

      if (!credentials.getUsername().equals(tokenUsername)) {
        return false;
      }

      char[] tmpPassword = credentials.getPassword();

      logger.trace("Attempting to validate token");
      if (tmpPassword.length > tokenMaxLength) {
        credentials.clearPassword();
        logger.error("Token was too long ({} characters)", tmpPassword.length);
        return false;
      }

      String password = String.valueOf(tmpPassword);
      credentials.clearPassword();

      authenticationSubject = authentication.validateToken(password, headers);
      if (authenticationSubject == null) {
        logger.error("Null credentials returned from authentication service");
        return false;
      }
    } catch (Exception e) {
      logger.error("Unable to validate token", e);
      return false;
    }

    return true;
  }
}
