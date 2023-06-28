package io.stargate.cql;

import io.quarkus.runtime.Startup;
import io.stargate.cql.impl.CqlImpl;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves as a wrapper around {@link CqlImpl} with start and stop lifecycle methods. */
@ApplicationScoped
@Startup
public class CqlService {

  // init slf4j logger
  private static final Logger logger = LoggerFactory.getLogger(CqlService.class);

  private final CqlImpl cqlImpl;

  @Inject
  public CqlService(CqlImpl cqlImpl) {
    this.cqlImpl = cqlImpl;
  }

  @PostConstruct
  public void start() {
    logger.info("Starting CQL service..");

    cqlImpl.start();

    logger.info("CQL service started..");
  }

  @PreDestroy
  public void stop() {
    logger.info("Stopping CQL service..");

    cqlImpl.stop();

    logger.info("CQL service stopped..");
  }
}
