package io.stargate.sgv2.docsapi.service.query.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(QueryExecutorDiffConsistencyTest.Profile.class)
class QueryExecutorDiffConsistencyTest extends AbstractValidatingStargateBridgeTest {

  public static class Profile extends MaxDepth4TestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("stargate.queries.consistency.reads", "ONE")
          .build();
    }
  }

  private final ExecutionContext context = ExecutionContext.NOOP_CONTEXT;

  @Inject QueryExecutor queryExecutor;

  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Inject DocumentProperties documentProperties;

  List<QueryOuterClass.ColumnSpec> columnSpec;

  List<QueryOuterClass.Value> row(String id, String p0, Double value) {
    return row(id, p0, "", "", value);
  }

  List<QueryOuterClass.Value> row(String id, String p0, String p1, String p2, Double value) {
    return ImmutableList.of(
        Values.of(id),
        Values.of(p0),
        Values.of(p1),
        Values.of(p2),
        Values.of(""),
        Values.of(value));
  }

  @BeforeEach
  public void initColumnSpec() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    // using only sub-set of columns
    columnSpec =
        schemaProvider
            .allColumnSpecStream()
            .filter(
                c -> {
                  String column = c.getName();

                  return Objects.equals(column, tableProps.keyColumnName())
                      || Objects.equals(column, tableProps.doubleValueColumnName())
                      || column.startsWith(tableProps.pathColumnPrefix());
                })
            .toList();
  }

  @Test
  public void fullScan() {
    QueryOuterClass.Query allDocsQuery =
        new QueryBuilder().select().star().from(schemaProvider.getTable().getName()).build();

    List<List<QueryOuterClass.Value>> rows =
        ImmutableList.of(row("1", "x", 1.0d), row("1", "y", 2.0d), row("2", "x", 3.0d));

    withQuery(allDocsQuery.getCql())
        .withColumnSpec(columnSpec)
        .withConsistency(QueryOuterClass.Consistency.ONE)
        .returning(rows);

    List<RawDocument> result =
        queryExecutor
            .queryDocs(allDocsQuery, 100, false, null, false, context)
            .subscribe()
            .withSubscriber(AssertSubscriber.create())
            .awaitNextItems(2)
            .awaitCompletion()
            .assertCompleted()
            .getItems();

    assertThat(result)
        .hasSize(2)
        .anySatisfy(
            doc -> {
              assertThat(doc.id()).isEqualTo("1");
              assertThat(doc.documentKeys()).containsExactly("1");
              assertThat(doc.rows()).hasSize(2);
            })
        .anySatisfy(
            doc -> {
              assertThat(doc.id()).isEqualTo("2");
              assertThat(doc.documentKeys()).containsExactly("2");
              assertThat(doc.rows()).hasSize(1);
            });
  }
}
