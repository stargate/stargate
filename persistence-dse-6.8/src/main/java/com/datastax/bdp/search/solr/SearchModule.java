package com.datastax.bdp.search.solr;

import static com.datastax.bdp.config.DseConfigYamlLoader.CONFIG_FILE_PROPERTY;

import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.router.InternalQueryRouterProtocol;
import com.datastax.bdp.search.solr.statements.SearchStatementFactory;
import com.google.inject.AbstractModule;
import org.apache.cassandra.cql3.Cql_DseSearchParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(SearchModule.class);

  @Override
  protected void configure() {
    logger.info("SearchModule.configure");

    // required for InternodeClient init
    System.setProperty(CONFIG_FILE_PROPERTY, "/dse.yaml");

    bind(InternodeMessaging.class);
    bind(InternodeProtocolRegistry.class).to(InternodeMessaging.class);
    bind(InternodeClient.class).toProvider(InternodeMessaging.class);

    bind(InternalQueryRouterProtocol.class);

    bind(Cql_DseSearchParser.SearchStatementFactory.class).to(SearchStatementFactory.class);

    requestStaticInjection(Cql_DseSearchParser.class);
  }
}
