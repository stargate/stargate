/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.schema.upgrade.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.CollectionUpgradeType;
import io.stargate.sgv2.docsapi.service.schema.query.CollectionQueryProvider;
import io.stargate.sgv2.docsapi.service.schema.upgrade.CollectionUpgradeAction;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * The action that deletes the existing indexes and re-creates them, ensuring they will be SAI
 * indexes.
 */
@ApplicationScoped
public class SaiIndexUpgradeAction implements CollectionUpgradeAction {

  @Inject StargateRequestInfo requestInfo;

  @Inject CollectionQueryProvider queryProvider;

  @Inject DataStoreProperties dataStoreProperties;

  /** {@inheritDoc} */
  @Override
  public CollectionUpgradeType getType() {
    return CollectionUpgradeType.SAI_INDEX_UPGRADE;
  }

  /** {@inheritDoc} */
  @Override
  public boolean canUpgrade(Schema.CqlTable collectionTable) {
    if (!dataStoreProperties.saiEnabled()) {
      return false;
    }

    List<Schema.CqlIndex> indexes = collectionTable.getIndexesList();
    boolean noCustom = indexes.stream().noneMatch(Schema.CqlIndex::getCustom);

    // If all secondary indexes are not SAI or there are no secondary indexes,
    // then an upgrade is available.
    return indexes.size() == 0 || noCustom;
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Void> executeUpgrade(
      Schema.CqlTable collectionTable, String namespace, String collection) {
    // get bridge from request
    StargateBridge bridge = requestInfo.getStargateBridge();

    return Uni.createFrom()
        .item(collectionTable)

        // make sure upgrade is possible
        .flatMap(
            table -> {
              boolean canUpgrade = canUpgrade(table);
              if (canUpgrade) {
                return Uni.createFrom().item(table);
              } else {
                String msg =
                    "Can not upgrade collection %s using the %s.".formatted(collection, getType());
                Exception error =
                    new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_UPGRADE_INVALID, msg);
                return Uni.createFrom().failure(error);
              }
            })

        // first delete indexes (what ever they are)
        .flatMap(
            table -> {
              List<QueryOuterClass.Query> dropQueries =
                  table.getIndexesList().stream()
                      .map(
                          index ->
                              queryProvider.deleteCollectionIndexQuery(namespace, index.getName()))
                      .toList();

              return Multi.createFrom()
                  .iterable(dropQueries)
                  .onItem()
                  .transformToUniAndMerge(bridge::executeQuery)
                  .collect()
                  .asList();
            })

        // then recreate with current data structure
        .flatMap(
            responses -> {
              List<QueryOuterClass.Query> createQueries =
                  queryProvider.createCollectionIndexQueries(namespace, collection);

              return Multi.createFrom()
                  .iterable(createQueries)
                  .onItem()
                  .transformToUniAndMerge(bridge::executeQuery)
                  .collect()
                  .asList()
                  .map(any -> null);
            });
  }
}
