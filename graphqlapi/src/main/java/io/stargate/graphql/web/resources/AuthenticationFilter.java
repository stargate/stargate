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
package io.stargate.graphql.web.resources;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.web.RequestToHeadersMapper;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * Performs authentication before each GraphQL request. The subject is stored as a request attribute
 * under {@link #SUBJECT_KEY}.
 */
@Provider
@Authenticated
public class AuthenticationFilter implements ContainerRequestFilter {

  public static final String SUBJECT_KEY = AuthenticationSubject.class.getName();
  public static final String DATA_STORE_KEY = DataStore.class.getName();

  private final AuthenticationService authenticationService;
  private final DataStoreFactory dataStoreFactory;

  public AuthenticationFilter(
      AuthenticationService authenticationService, DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.dataStoreFactory = dataStoreFactory;
  }

  @Override
  public void filter(ContainerRequestContext context) {
    String token = context.getHeaderString("X-Cassandra-Token");
    try {
      Map<String, String> headers = deduplicate(context.getHeaders());
      AuthenticationSubject subject = authenticationService.validateToken(token, headers);
      context.setProperty(SUBJECT_KEY, subject);

      DataStoreOptions dataStoreOptions =
          DataStoreOptions.builder()
              .putAllCustomProperties(headers)
              .defaultParameters(CassandraFetcher.DEFAULT_PARAMETERS)
              .alwaysPrepareQueries(true)
              .build();
      DataStore dataStore = dataStoreFactory.create(subject.asUser(), dataStoreOptions);
      context.setProperty(DATA_STORE_KEY, dataStore);
    } catch (UnauthorizedException e) {
      context.abortWith(
          Response.status(Response.Status.UNAUTHORIZED)
              .entity(buildError("Authentication required", context.getMediaType()))
              .build());
    }
  }

  /** Same logic as {@link RequestToHeadersMapper}: only keep the first entry for each key. */
  private Map<String, String> deduplicate(MultivaluedMap<String, String> multimap) {
    Map<String, String> map = new HashMap<>();
    for (String key : multimap.keySet()) {
      map.put(key, multimap.getFirst(key));
    }
    return map;
  }

  private static Object buildError(String message, MediaType mediaType) {
    return mediaType != null
            && ("json".equals(mediaType.getSubtype()) || "graphql".equals(mediaType.getSubtype()))
        ? ImmutableMap.of("errors", ImmutableList.of(message))
        : message;
  }
}
