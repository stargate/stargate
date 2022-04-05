package com.datastax.bdp.search.solr.core;

import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.router.InternalQueryRouter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class StargateCoreContainer {
  private static final Logger logger = LoggerFactory.getLogger(StargateCoreContainer.class);
  private volatile InternodeClient messagingClient;
  private volatile InternalQueryRouter queryRouter;

  private static StargateCoreContainer _instance;

  public static StargateCoreContainer getInstance() {
    return _instance;
  }

  public static void setInstance(StargateCoreContainer instance) {
    _instance = instance;
  }

  public InternodeClient getInternodeMessagingClient() {
    return messagingClient;
  }

  public InternalQueryRouter getQueryRouter() {
    return queryRouter;
  }

  @Inject
  public StargateCoreContainer(InternodeClient messagingClient) {
    this.messagingClient = messagingClient;
    queryRouter = new InternalQueryRouter(messagingClient);
  }
}
