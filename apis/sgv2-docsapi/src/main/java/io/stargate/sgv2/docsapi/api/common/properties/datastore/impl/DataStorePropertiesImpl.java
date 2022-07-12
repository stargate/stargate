package io.stargate.sgv2.docsapi.api.common.properties.datastore.impl;

import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;

/**
 * Immutable implementation of the {@link DataStoreProperties}.
 *
 * @see DataStoreProperties
 */
public record DataStorePropertiesImpl(
    boolean secondaryIndexesEnabled, boolean saiEnabled, boolean loggedBatchesEnabled)
    implements DataStoreProperties {}
