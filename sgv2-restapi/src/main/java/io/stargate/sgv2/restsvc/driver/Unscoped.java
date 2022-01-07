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
package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.CqlSession;
import org.glassfish.hk2.api.AnnotationLiteral;

/**
 * Annotates a {@link CqlSession} injection to signify that it should not be scoped to the current
 * web user.
 *
 * <p>Namely, given:
 *
 * <pre>{@code
 * @Inject CqlSession session1;
 * @Inject @Unscoped CqlSession session2;
 * }</pre>
 *
 * <ul>
 *   <li>for {@code session1}, Stargate will look for an auth token and optional tenant ID in the
 *       HTTP request, and every CQL request executed through the session will be authentified and
 *       scoped to this particular tenant.
 *   <li>for {@code session2}, no authentication will occur and requests will be global to all
 *       tenants.
 * </ul>
 *
 * Note that there is only one actual driver instance; scoped sessions are implemented with a
 * wrapper.
 *
 * @see ScopedCqlSession
 */
public @interface Unscoped {
  AnnotationLiteral<Unscoped> LITERAL = new UnscopedLiteral();
}
