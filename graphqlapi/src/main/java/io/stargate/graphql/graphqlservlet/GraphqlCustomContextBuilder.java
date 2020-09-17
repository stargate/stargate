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
