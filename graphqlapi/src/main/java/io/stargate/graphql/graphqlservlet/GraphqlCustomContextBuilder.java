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
package io.stargate.graphql.graphqlservlet;

import graphql.kickstart.execution.context.DefaultGraphQLContextBuilder;
import graphql.kickstart.execution.context.GraphQLContext;
import graphql.kickstart.servlet.context.GraphQLServletContextBuilder;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.Session;
import javax.websocket.server.HandshakeRequest;
import org.dataloader.DataLoaderRegistry;

public class GraphqlCustomContextBuilder extends DefaultGraphQLContextBuilder
    implements GraphQLServletContextBuilder {
  @Override
  public GraphQLContext build(HttpServletRequest request, HttpServletResponse response) {
    return new HTTPAwareContextImpl(new DataLoaderRegistry(), request, response);
  }

  @Override
  public GraphQLContext build(Session session, HandshakeRequest handshakeRequest) {
    return new HTTPAwareContextImpl(new DataLoaderRegistry(), session, handshakeRequest);
  }
}
