package io.stargate.db;

import java.util.List;

import org.apache.cassandra.stargate.locator.InetAddressAndPort;

public interface EventListener
{
    default void onCreateKeyspace(String keyspace)
    {
    }

    default void onCreateTable(String keyspace, String table)
    {
    }

    default void onCreateView(String keyspace, String view)
    {
        onCreateTable(keyspace, view);
    }

    default void onCreateType(String keyspace, String type)
    {
    }

    default void onCreateFunction(String keyspace, String function, List<String> argumentTypes)
    {
    }

    default void onCreateAggregate(String keyspace, String aggregate, List<String> argumentTypes)
    {
    }

    default void onAlterKeyspace(String keyspace)
    {
    }

    default void onAlterTable(String keyspace, String table)
    {
    }

    default void onAlterView(String keyspace, String view)
    {
        onAlterTable(keyspace, view);
    }

    default void onAlterType(String keyspace, String type)
    {
    }

    default void onAlterFunction(String keyspace, String function, List<String> argumentTypes)
    {
    }

    default void onAlterAggregate(String keyspace, String aggregate, List<String> argumentTypes)
    {
    }

    default void onDropKeyspace(String keyspace)
    {
    }

    default void onDropTable(String keyspace, String table)
    {
    }

    default void onDropView(String keyspace, String view)
    {
        onDropTable(keyspace, view);
    }

    default void onDropType(String keyspace, String type)
    {
    }

    default void onDropFunction(String keyspace, String function, List<String> argumentTypes)
    {
    }

    default void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes)
    {
    }

    default void onJoinCluster(InetAddressAndPort endpoint)
    {
    }

    default void onLeaveCluster(InetAddressAndPort endpoint)
    {
    }

    default void onUp(InetAddressAndPort endpoint)
    {
    }

    default void onDown(InetAddressAndPort endpoint)
    {
    }

    default void onMove(InetAddressAndPort endpoint)
    {
    }
}
