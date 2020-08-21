/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.io.Serializable;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class ColumnMappingMetadata implements Serializable
{
    public abstract String vertexColumn();

    public abstract String edgeColumn();

    public static ColumnMappingMetadata create(String vertexColumn, String edgeColumn)
    {
        return ImmutableColumnMappingMetadata.builder().vertexColumn(vertexColumn).edgeColumn(edgeColumn).build();
    }
}
