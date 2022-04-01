package com.datastax.bdp.search.solr.core;

import com.datastax.bdp.system.SolrKeyspace;
import com.datastax.bdp.util.CassandraUtil;
import com.datastax.bdp.util.SchemaTool;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrCoreResourceManager {
  private static final Logger logger = LoggerFactory.getLogger(SolrCoreResourceManager.class);
  public static final String BACKUP_SUFFIX = ".bak";
  private static volatile SolrCoreResourceManager instance = new SolrCoreResourceManager();

  public static SolrCoreResourceManager getInstance() {
    return instance;
  }

  protected SolrCoreResourceManager() {}

  public ByteBuffer tryReadResource(String coreName, String resourceName) {
    try {
      return getResource(coreName, resourceName, false);
    } catch (Throwable e) {
      logger.error(e.getMessage(), e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage());
    }
  }

  private ByteBuffer getResource(String coreName, String resourceName, boolean failOnMissing)
      throws IOException {
    ByteBuffer resourceValue = null;
    try {
      UntypedResultSet result = getAdminSchemaResources(coreName, resourceName);
      if (!result.isEmpty()) {
        resourceValue = result.one().getBlob(SolrKeyspace.RESOURCE_VALUE_COLUMN);
        logger.info(
            String.format("Successfully loaded resource %s for core %s", resourceName, coreName));
      }
    } catch (Throwable e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }

    // If resource is still null, eventually give up:
    if (resourceValue == null) {
      if (failOnMissing) {
        String error =
            String.format(
                "No resource %s for core %s, did you miss to upload it?", resourceName, coreName);
        logger.error(error);
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, error);
      } else {
        logger.info(
            String.format(
                "No resource %s found for core %s on any live node.", resourceName, coreName));
      }
    }

    return resourceValue;
  }

  private UntypedResultSet getAdminSchemaResources(String coreName, String resourceName)
      throws IOException {
    UntypedResultSet result = UntypedResultSet.EMPTY;
    try {
      // Try to read from admin keyspace:
      if (isAdminSchemaCreated()) {
        String resourceQuery = null;
        if (resourceName != null) {
          resourceQuery =
              String.format(
                  "SELECT resource_value FROM %s WHERE core_name = '%s' and resource_name = '%s'",
                  String.format("%s.%s", SolrKeyspace.NAME, SolrKeyspace.SOLR_RESOURCES),
                  coreName,
                  resourceName);
        } else {
          resourceQuery =
              String.format(
                  "SELECT resource_name FROM %s WHERE core_name = '%s'",
                  String.format("%s.%s", SolrKeyspace.NAME, SolrKeyspace.SOLR_RESOURCES), coreName);
        }

        /**
         * We try to read with the consistency level degrading from QUORUM -> LOCAL_QUORUM ->
         * LOCAL_ONE. Given that resources are written at a minimum of LOCAL_QUORUM we should be
         * safe, except for two subtle cases:
         *
         * <p>1) When some node creates a core for the first time, the schema propagation loads the
         * core on other nodes before we have the chance to do anything (that's why we
         * waitForIndex), so LOCAL_QUORUM reads are pretty much needed, but we can't mandate for
         * them as the same code path is activated during startup; by the way, the common case is
         * writing resources and creating cores with all nodes up, so we should always have the
         * LOCAL_QUORUM.
         *
         * <p>2) When some node writes a resource and then all nodes are stopped, we have no 100%
         * guarantee the first started node will see the just written resource, because of the
         * LOCAL_QUORUM write and LOCAL_ONE read, but we have to live with this; shouldn't be
         * particularly worrying, as writing a resource without actually creating/reloading a core
         * is pretty uncommon.
         */
        result =
            CassandraUtil.robustCql3Statement(
                ClientState.forInternalCalls(),
                resourceQuery,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.LOCAL_ONE);

        if (result.isEmpty()) {
          logger.info(
              String.format(
                  "No resource%s found for core %s by querying from local node.",
                  resourceName == null ? "s" : " " + resourceName, coreName));
        } else {
          logger.info(
              String.format(
                  "Successfully loaded resource%s for core %s by querying from local node.",
                  resourceName == null ? "s" : " " + resourceName, coreName));
        }
      } else {
        logger.info(
            String.format(
                "No %s keyspace or %s column family found.",
                SolrKeyspace.NAME, SolrKeyspace.SOLR_RESOURCES));
      }
    } catch (Throwable e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }

    return result;
  }

  private boolean isAdminSchemaCreated() {
    return SchemaTool.cql3KeyspaceExists(SolrKeyspace.NAME)
        && SchemaTool.cql3TableExists(SolrKeyspace.NAME, SolrKeyspace.SOLR_RESOURCES)
        && SchemaTool.metadataExists(SolrKeyspace.NAME, SolrKeyspace.SOLR_RESOURCES);
  }
}
