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
package io.stargate.grpc.impl;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcImpl {
  private static final Logger logger = LoggerFactory.getLogger(GrpcImpl.class);

  private final Persistence persistence;
  private final Metrics metrics;
  private final AuthenticationService authentication;

  public GrpcImpl(
      Persistence persistence,
      Metrics metrics,
      AuthenticationService authentication) {
    this.persistence = persistence;
    this.metrics = metrics;
    this.authentication = authentication;
  }

  public void start() {
    persistence.setRpcReady(true);
  }

  public void stop() {
    persistence.setRpcReady(false);
  }
}
