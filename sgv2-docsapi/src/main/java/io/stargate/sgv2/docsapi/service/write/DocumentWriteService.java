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

package io.stargate.sgv2.docsapi.service.write;

import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.config.QueriesConfig;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.service.write.db.InsertQueryBuilder;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class DocumentWriteService {

  private final StargateRequestInfo requestInfo;
  private final TimeSource timeSource;
  private final InsertQueryBuilder insertQueryBuilder;
  private final boolean useLoggedBatches;
  private final QueriesConfig queriesConfig;

  @Inject
  public DocumentWriteService(
      StargateRequestInfo requestInfo,
      TimeSource timeSource,
      DataStoreProperties dataStoreProperties,
      DocumentProperties documentProperties,
      QueriesConfig queriesConfig) {
    this.requestInfo = requestInfo;
    this.insertQueryBuilder = new InsertQueryBuilder(documentProperties);
    this.timeSource = timeSource;
    this.useLoggedBatches = dataStoreProperties.loggedBatchesEnabled();
    this.queriesConfig = queriesConfig;
  }

  /**
   * Writes a single document, without deleting the existing document with the same key. Please call
   * this method only if you are sure that the document with given ID does not exist.
   *
   * @param keyspace Keyspace to store document in.
   * @param collection Collection the document belongs to.
   * @param documentId Document ID.
   * @param rows Rows of this document.
   * @param ttl the time-to-live of the rows (seconds)
   * @param numericBooleans If numeric boolean should be stored.
   * @param context Execution content for profiling.
   * @return Single containing the {@link ResultSet} of the batch execution.
   */
  @WithSpan
  public Uni<ResultSet> writeDocument(
      String keyspace,
      String collection,
      String documentId,
      List<JsonShreddedRow> rows,
      Integer ttl,
      boolean numericBooleans,
      ExecutionContext context) {

    StargateBridge bridge = requestInfo.getStargateBridge();

    return Uni.createFrom()
        .item(() -> insertQueryBuilder.buildQuery(keyspace, collection, ttl))
        .map(
            query -> {
              long timestamp = timeSource.currentTimeMicros();
              return rows.stream()
                  .map(
                      row ->
                          insertQueryBuilder.bind(
                              query, documentId, row, timestamp, numericBooleans))
                  .toList();
            })
        .flatMap(
            boundQueries -> executeBatch(bridge, boundQueries, context.nested("ASYNC INSERT")));
  }

  private Uni<ResultSet> executeBatch(
      StargateBridge bridge, List<QueryOuterClass.Query> boundQueries, ExecutionContext context) {

    // trace queries in context
    boundQueries.forEach(context::traceDeferredDml);

    // then execute batch
    Batch.Type type = useLoggedBatches ? Batch.Type.LOGGED : Batch.Type.UNLOGGED;
    Batch.Builder batch =
        Batch.newBuilder()
            .setType(type)
            .setParameters(
                QueryOuterClass.BatchParameters.newBuilder()
                    .setConsistency(
                        QueryOuterClass.ConsistencyValue.newBuilder()
                            .setValue(queriesConfig.consistency().reads())));
    boundQueries.forEach(
        query ->
            batch.addQueries(
                QueryOuterClass.BatchQuery.newBuilder()
                    .setCql(query.getCql())
                    .setValues(query.getValues())));

    return bridge.executeBatch(batch.build()).map(QueryOuterClass.Response::getResultSet);
  }
}
