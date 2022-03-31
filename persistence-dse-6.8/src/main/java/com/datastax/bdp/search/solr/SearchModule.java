package com.datastax.bdp.search.solr;

import static com.datastax.bdp.config.DseConfigYamlLoader.CONFIG_FILE_PROPERTY;

import com.datastax.bdp.cassandra.cql3.DseQueryOperationFactory;
import com.datastax.bdp.cassandra.cql3.SolrQueryOperationFactory;
import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.router.InternalQueryRouterProtocol;
import com.google.inject.AbstractModule;
import org.apache.cassandra.cql3.Cql_DseSearchParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchModule extends AbstractModule {
  private static final Logger logger = LoggerFactory.getLogger(SearchModule.class);

  @Override
  protected void configure() {
    System.setProperty(CONFIG_FILE_PROPERTY, "/dse.yaml");

    bind(InternodeMessaging.class);
    bind(InternodeProtocolRegistry.class).to(InternodeMessaging.class);
    bind(InternodeClient.class).toProvider(InternodeMessaging.class);
    bind(InternalQueryRouterProtocol.class);
    // bind(Cql_DseSearchParser.SearchStatementFactory.class).to(SearchStatementFactory.class);
    bind(DseQueryOperationFactory.class).to(SolrQueryOperationFactory.class);
    requestStaticInjection(Cql_DseSearchParser.class);
    requestInjection(DseQueryOperationFactory.class);
  }
}
