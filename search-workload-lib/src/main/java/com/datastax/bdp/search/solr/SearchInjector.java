package com.datastax.bdp.search.solr;

import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.router.InternalQueryRouterProtocol;
import com.datastax.bdp.search.solr.core.StargateCoreContainer;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchInjector {
  private static final Logger logger = LoggerFactory.getLogger(SearchInjector.class);

  private static volatile Injector instance;

  private static volatile AbstractModule module;

  private static Injector get() {
    if (instance == null) {
      synchronized (SearchInjector.class) {
        if (instance == null) {
          instance = Guice.createInjector(getModule());
        }
      }
    }
    return instance;
  }

  private static AbstractModule getModule() {
    if (module == null) {
      module = new SearchModule();
    }
    return module;
  }

  private static synchronized void set(Injector injector) {
    instance = injector;
  }

  public static void initSearch() {
    logger.info("Initializing Search Functionality");

    Injector injector = SearchInjector.get();

    InternodeMessaging internodeMessaging = injector.getInstance(InternodeMessaging.class);
    internodeMessaging.register(injector.getInstance(InternalQueryRouterProtocol.class));
    internodeMessaging.activate();

    StargateCoreContainer.setInstance(injector.getInstance(StargateCoreContainer.class));
  }
}
