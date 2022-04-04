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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.common.auth;

import io.vertx.ext.web.RoutingContext;

import java.util.Optional;

/**
 * Resolver of the Cassandra token. This token will be passed to the Bridge.
 * <p>
 * The implementation can use any information from the {@link RoutingContext} to obtain the token.
 */
@FunctionalInterface
public interface CassandraTokenResolver {

    /**
     * Returns a Cassandra token given a RoutingContext.
     *
     * @param context the routing context
     * @return The Cassandra token to pass to the Bridge. If empty, indicates that no token should be used..
     */
    Optional<String> resolve(RoutingContext context);

}
