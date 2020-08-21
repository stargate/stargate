/**
 * Copyright DataStax, Inc.
 * <p>
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.common.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageProxy;

/**
 * Utility methods to manipulate Cassandra schema.
 */
public class SchemaTool
{
    private static Logger logger = LoggerFactory.getLogger(SchemaTool.class);

    /**
     * Checks it the output of StorageProxy.describeSchemaVersions indicates schema agreement.
     */
    public static boolean isSchemaAgreement(Map<String, List<String>> schemaVersions)
    {
        final int size = schemaVersions.size();

        if (size == 1)
        {
            // the map is from schemaversions -> nodes' belief state.  1 schema version -> we are good
            logger.debug("isSchemaAgreement detected only one version; returning true");
            return true;
        }
        else if (size == 2 && schemaVersions.containsKey(StorageProxy.UNREACHABLE))
        {
            boolean agreed = true;
            // all reachable nodes agree on the same schema version; the question is whether the unreachable
            // nodes are dead/leaving/hibernating/etc or just unreachable
            for (String ip : schemaVersions.get(StorageProxy.UNREACHABLE))
            {
                final EndpointState es = Gossiper.instance.getEndpointStateForEndpoint(getByName(ip));
                final boolean isDead = Gossiper.instance.isDeadState(es);
                agreed &= isDead;
                logger.debug("Node {}: isDeadState: {}, EndpointState: {}", ip, isDead, es);
            }
            logger.debug("isSchemaAgreement returning {}", agreed);
            return agreed;
        }
        else
        {
            logger.debug("isSchemaAgreement returning false; schemaVersions.size(): {}", schemaVersions.size());
            return false;
        }
    }

    private static InetAddress getByName(String str)
    {
        try
        {
            return InetAddress.getByName(str);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
