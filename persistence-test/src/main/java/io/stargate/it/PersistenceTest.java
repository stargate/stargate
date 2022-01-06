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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it;

import static com.datastax.oss.driver.api.core.uuid.Uuids.random;
import static com.datastax.oss.driver.api.core.uuid.Uuids.timeBased;
import static io.stargate.db.query.Predicate.EQ;
import static io.stargate.db.query.Predicate.IN;
import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Static;
import static io.stargate.db.schema.Column.Order.ASC;
import static io.stargate.db.schema.Column.Order.DESC;
import static io.stargate.db.schema.Column.Type.Ascii;
import static io.stargate.db.schema.Column.Type.Bigint;
import static io.stargate.db.schema.Column.Type.Blob;
import static io.stargate.db.schema.Column.Type.Boolean;
import static io.stargate.db.schema.Column.Type.Date;
import static io.stargate.db.schema.Column.Type.Decimal;
import static io.stargate.db.schema.Column.Type.Double;
import static io.stargate.db.schema.Column.Type.Duration;
import static io.stargate.db.schema.Column.Type.Float;
import static io.stargate.db.schema.Column.Type.Inet;
import static io.stargate.db.schema.Column.Type.Int;
import static io.stargate.db.schema.Column.Type.List;
import static io.stargate.db.schema.Column.Type.Map;
import static io.stargate.db.schema.Column.Type.Set;
import static io.stargate.db.schema.Column.Type.Smallint;
import static io.stargate.db.schema.Column.Type.Text;
import static io.stargate.db.schema.Column.Type.Time;
import static io.stargate.db.schema.Column.Type.Timestamp;
import static io.stargate.db.schema.Column.Type.Timeuuid;
import static io.stargate.db.schema.Column.Type.Tinyint;
import static io.stargate.db.schema.Column.Type.Tuple;
import static io.stargate.db.schema.Column.Type.Uuid;
import static io.stargate.db.schema.Column.Type.Varint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.stargate.db.ComparableKey;
import io.stargate.db.PagingPosition;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.RowDecorator;
import io.stargate.db.SimpleStatement;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PersistenceDataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.MaterializedView;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.ExternalStorage;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.NotThreadSafe;
import org.assertj.core.api.Assertions;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ExternalStorage.class)
@ClusterSpec(shared = true)
@NotThreadSafe
public abstract class PersistenceTest {
  private static final Logger logger = LoggerFactory.getLogger(PersistenceTest.class);

  static {
    // Required by the testWarnings() so a warning is generated (rather than a rejection). Unlikely
    // to ever influence another test. This needs to be really early, before the concrete
    // implementations initialize their persistence so it gets picked up.
    System.setProperty("cassandra.expiration_date_overflow_policy", "CAP");
  }

  protected static final int KEYSPACE_NAME_MAX_LENGTH = 48;

  private DataStore dataStore;
  private String table;
  private String keyspace;
  private ClusterConnectionInfo backend;

  private static final int CUSTOM_PAGE_SIZE = 50;

  protected abstract Persistence persistence();

  @BeforeEach
  public void setup(TestInfo testInfo, ClusterConnectionInfo backend) {
    this.backend = backend;

    dataStore = new PersistenceDataStoreFactory(persistence()).createInternal();
    logger.info("{}", dataStore);

    Optional<String> name = testInfo.getTestMethod().map(Method::getName);
    assertThat(name).isPresent();
    String testName = name.get();

    keyspace = "ks_persistence_" + System.currentTimeMillis() + "_" + testName;
    if (keyspace.length() > KEYSPACE_NAME_MAX_LENGTH) {
      keyspace = keyspace.substring(0, KEYSPACE_NAME_MAX_LENGTH);
    }

    table = testName;
  }

  @Test
  public void querySystemTables() throws ExecutionException, InterruptedException {
    Future<ResultSet> rs =
        dataStore
            .queryBuilder()
            .select()
            .column("cluster_name")
            .column("data_center")
            .from("system", "local")
            .build()
            .execute();
    Row row = rs.get().one();

    logger.info(String.valueOf(row));
    assertThat(row).isNotNull();
    assertThat(row.columns().get(0).name()).isEqualTo("cluster_name");
    assertThat(row.columns().get(1).name()).isEqualTo("data_center");
    assertThat(row.getString("cluster_name")).isEqualTo(backend.clusterName());
    assertThat(row.getString("data_center")).isEqualTo(backend.datacenter());

    rs =
        dataStore
            .queryBuilder()
            .select()
            .column("data_center")
            .from("system", "peers")
            .build()
            .execute();
    // As our modified local/peers table only include stargate nodes, we shouldn't have anyone
    // in peers.
    assertThat(rs.get().hasNoMoreFetchedRows()).isTrue();
  }

  @Test
  public void testKeyspace() throws ExecutionException, InterruptedException {
    createKeyspace();
  }

  @Test
  public void testAlterAndDrop() throws ExecutionException, InterruptedException {
    createKeyspace();
    //    ResultSet createTable =
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("created", Boolean, PartitionKey)
        .build()
        .execute()
        .join();

    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    // assertThat(createTable.waitedForSchemaAgreement()).isTrue();
    Table t = dataStore.schema().keyspace(keyspace).table(table);
    assertThat(t)
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("created", Boolean, PartitionKey)
                .build()
                .keyspace(keyspace)
                .table(table));

    //    ResultSet alterTable =
    dataStore
        .queryBuilder()
        .alter()
        .table(keyspace, table)
        .addColumn("added", Boolean)
        .build()
        .execute()
        .join();
    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    //        assertThat(alterTable.waitedForSchemaAgreement()).isTrue();
    assertThat(dataStore.schema().keyspace(keyspace).table(table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("created", Boolean, PartitionKey)
                .column("added", Boolean)
                .build()
                .keyspace(keyspace)
                .table(table));

    //    ResultSet dropTable =
    dataStore.queryBuilder().drop().table(keyspace, table).build().execute().join();
    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    //        assertThat(dropTable.waitedForSchemaAgreement()).isTrue();
    assertThat(dataStore.schema().keyspace(keyspace).table(table)).isNull();
  }

  @Test
  public void testColumnKinds() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("PK1", Text, PartitionKey)
        .column("PK2", Text, PartitionKey)
        .column("CC1", Text, Clustering, ASC)
        .column("CC2", Text, Clustering, DESC)
        .column("R1", Text)
        .column("R2", Text)
        .column("S1", Text, Static)
        .column("S2", Text, Static)
        .build()
        .execute()
        .join();
    assertThat(dataStore.schema().keyspace(keyspace).table(table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("PK1", Text, PartitionKey)
                .column("PK2", Text, PartitionKey)
                .column("CC1", Text, Clustering, ASC)
                .column("CC2", Text, Clustering, DESC)
                .column("S1", Text, Static)
                .column("S2", Text, Static)
                .column("R1", Text)
                .column("R2", Text)
                .build()
                .keyspace(keyspace)
                .table(table));
  }

  @Disabled("Disabling for now since it currently just hangs")
  @Test
  public void testInsertingAndReadingDifferentTypes() throws Exception {
    //    Keyspace ks =
    createKeyspace();

    Column.ColumnType nestedTuple = Tuple.of(Int, Double);
    Column.ColumnType tupleType = Tuple.of(Text, nestedTuple);
    Column.ColumnType nestedDuration = Tuple.of(Int, Duration);
    Column.ColumnType tupleTypeWithDuration = Tuple.of(Text, nestedDuration);
    //        Column.ColumnType nestedGeo = Tuple.of(Polygon, LineString);
    //        Column.ColumnType tupleTypeWithGeo = Tuple.of(Point, nestedGeo);

    //        Point point = new Point(3.3, 4.4);
    //        Polygon polygon = new Polygon(new Point(30, 10), new Point(10, 20), new Point(20, 40),
    // new Point(40, 40));
    //        LineString lineString = new LineString(new Point(30, 10), new Point(40, 40), new
    // Point(20, 40));

    // TODO: [doug] 2020-07-13, Mon, 9:26 Having trouble with UDTs, removing for now
    //        UserDefinedType udtType =
    // ImmutableUserDefinedType.builder().name("My_udt").keyspace(ks.name())
    //                .addColumns(Column.create("a", Int), Column.create("b", Int),
    // Column.create("c", Text)).build();

    List<Pair<Column.ColumnType, Object>> values =
        ImmutableList.<Pair<Column.ColumnType, Object>>builder()
            // Non frozen types
            .add(Pair.with(Ascii, "hi"))
            .add(Pair.with(Bigint, 23L))
            .add(Pair.with(Blob, ByteBuffer.wrap(new byte[] {2, 3})))
            .add(Pair.with(Boolean, true))
            .add(Pair.with(Date, LocalDate.ofEpochDay(34)))
            .add(Pair.with(Decimal, BigDecimal.valueOf(2.3)))
            .add(Pair.with(Double, 3.8))
            .add(Pair.with(Double, java.lang.Double.NaN))
            .add(Pair.with(Double, java.lang.Double.NEGATIVE_INFINITY))
            .add(Pair.with(Double, java.lang.Double.POSITIVE_INFINITY))
            .add(Pair.with(Duration, CqlDuration.newInstance(2, 3, 5)))
            .add(Pair.with(Float, 4.9f))
            .add(Pair.with(Float, java.lang.Float.NaN))
            .add(Pair.with(Float, java.lang.Float.NEGATIVE_INFINITY))
            .add(Pair.with(Float, java.lang.Float.POSITIVE_INFINITY))
            .add(Pair.with(Inet, Inet4Address.getByAddress(new byte[] {2, 3, 4, 5})))
            .add(Pair.with(Int, 4))
            .add(Pair.with(List.of(Double), Arrays.asList(3.0, 4.5)))
            .add(Pair.with(Map.of(Text, Int), ImmutableMap.of("Alice", 3, "Bob", 4)))
            .add(Pair.with(Set.of(Double), ImmutableSet.of(3.4, 5.3)))
            .add(
                Pair.with(
                    List.of(Duration),
                    Arrays.asList(
                        CqlDuration.newInstance(2, 3, 5), CqlDuration.newInstance(2, 3, 6))))
            .add(
                Pair.with(
                    Map.of(Text, Duration),
                    ImmutableMap.of(
                        "Alice",
                        CqlDuration.newInstance(2, 3, 5),
                        "Bob",
                        CqlDuration.newInstance(2, 3, 6))))
            .add(Pair.with(Smallint, (short) 7))
            .add(Pair.with(Time, LocalTime.ofSecondOfDay(20)))
            .add(Pair.with(Timestamp, Instant.ofEpochSecond(3)))
            .add(Pair.with(Timeuuid, timeBased()))
            .add(Pair.with(Tinyint, (byte) 4))
            .add(Pair.with(tupleType, tupleType.create("Test", nestedTuple.create(2, 3.0))))
            .add(
                Pair.with(
                    tupleTypeWithDuration,
                    tupleTypeWithDuration.create(
                        "Test", nestedDuration.create(2, CqlDuration.newInstance(2, 3, 6)))))
            .add(Pair.with(Uuid, random()))
            .add(Pair.with(Text, "Hi"))
            .add(Pair.with(Varint, BigInteger.valueOf(23)))
            //                .add(Pair.with(Point, point))
            //                .add(Pair.with(Polygon, polygon))
            //                .add(Pair.with(LineString, lineString))
            //                .add(Pair.with(Map.of(Point, Polygon), ImmutableMap.of(point,
            // polygon)))
            //                .add(Pair.with(Map.of(LineString, Polygon),
            // ImmutableMap.of(lineString, polygon)))
            //                .add(Pair.with(Set.of(Point), ImmutableSet.of(point, new Point(3, 3),
            // new Point(100, 100))))
            //                .add(Pair.with(Set.of(Polygon), ImmutableSet.of(polygon, polygon,
            // polygon)))
            //                .add(Pair.with(Set.of(LineString), ImmutableSet.of(lineString,
            // lineString, lineString)))
            //                .add(Pair.with(List.of(Point), ImmutableList.of(point, new Point(-100,
            // -200), new Point(3.0, 7.0))))
            //                .add(Pair.with(List.of(Polygon), ImmutableList.of(polygon, polygon,
            // polygon)))
            //                .add(Pair.with(List.of(LineString), ImmutableList.of(lineString,
            // lineString, lineString)))
            //                .add(Pair.with(tupleTypeWithGeo, tupleTypeWithGeo.create(point,
            // nestedGeo.create(polygon, lineString))))
            // TODO: [doug] 2020-07-13, Mon, 9:26 Having trouble with UDTs, removing for now
            //                .add(Pair.with(udtType, udtType.create(23, 45, "some text")))

            // Frozen types
            .add(Pair.with(List.of(Double).frozen(), Arrays.asList(3.0, 4.5)))
            .add(Pair.with(Map.of(Text, Int).frozen(), ImmutableMap.of("Alice", 3, "Bob", 4)))
            .add(Pair.with(Set.of(Double).frozen(), ImmutableSet.of(3.4, 5.3)))
            .build();

    Supplier<Stream<Column.ColumnType>> distinctColumnTypesStream =
        () -> values.stream().map(Pair::getValue0).distinct();

    // TODO: [doug] 2020-07-13, Mon, 9:27 Having trouble with UDTs, removing for now
    //        dataStore.query().create().type(keyspace, udtType).ifNotExists().execute();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("PK", Text, PartitionKey)
        .column(
            distinctColumnTypesStream
                .get()
                .map(columnType -> Column.create(columnName(columnType), columnType))
                .collect(Collectors.toList()))
        .build()
        .execute()
        .join();

    // check the columns and their types are correctly created
    distinctColumnTypesStream
        .get()
        .forEach(
            columnType -> {
              assertThat(
                      dataStore
                          .schema()
                          .keyspace(keyspace)
                          .table(table)
                          .column(columnName(columnType)))
                  .withFailMessage(
                      "Table did not contain definition for column %s", columnType.cqlDefinition())
                  .isNotNull();
              assertThat(
                      dataStore
                          .schema()
                          .keyspace(keyspace)
                          .table(table)
                          .column(columnName(columnType))
                          .type())
                  .isEqualToComparingFieldByField(columnType);
            });

    // check we can insert data and retrieve the same data
    for (int i = 0; i < values.size(); i++) {
      Pair<Column.ColumnType, Object> pair = values.get(i);
      dataStore
          .queryBuilder()
          .insertInto(keyspace, table)
          .value("PK", "PK" + i)
          .value(columnName(pair.getValue0()), pair.getValue1())
          .build()
          .execute()
          .join();

      Row row =
          dataStore
              .queryBuilder()
              .select()
              .star()
              .from(keyspace, table)
              .where("PK", EQ, "PK" + i)
              .build()
              .execute()
              .join()
              .one();

      assertThat(row.getObject(columnName(pair.getValue0()))).isEqualTo(pair.getValue1());
    }
  }

  private String columnName(Column.ColumnType type) {
    return "c" + System.identityHashCode(type);
  }

  /**
   * This will make sure that we can insert data and use it in a SELECT * FROM ks.tbl query. Under
   * the hood this will test every type's toInternalValue() / toExternalValue() methods.
   */
  @Test
  public void testUseTypesInWhere() throws Exception {
    createKeyspace();
    Map<Column.ColumnType, Object> values =
        ImmutableMap.<Column.ColumnType, Object>builder()
            .put(Ascii, "hi")
            .put(Bigint, 23L)
            .put(Blob, ByteBuffer.wrap(new byte[] {2, 3}))
            .put(Boolean, true)
            // .put(Counter, 23) // can't be used as PK
            .put(Date, LocalDate.ofEpochDay(34))
            .put(Decimal, BigDecimal.valueOf(2.3))
            .put(Double, 3.8d)
            // .put(Duration, com.datastax.driver.core.Duration.newInstance(2, 3, 5)) // can't be
            // used as PK
            .put(Float, 4.9f)
            .put(Inet, Inet4Address.getByAddress(new byte[] {2, 3, 4, 5}))
            .put(Int, 4)
            .put(Smallint, (short) 7)
            .put(Text, "some text")
            .put(Time, LocalTime.ofSecondOfDay(20))
            .put(Timestamp, Instant.ofEpochSecond(3))
            .put(Timeuuid, timeBased())
            .put(Tinyint, (byte) 4)
            .put(Uuid, random())
            .put(Varint, BigInteger.valueOf(23))
            //                .put(Point, new Point(3.3, 4.4))
            //                .put(Polygon, new Polygon(new Point(30, 10), new Point(10, 20), new
            // Point(20, 40), new Point(40, 40)))
            //                .put(LineString, new LineString(new Point(30, 10), new Point(40, 40),
            // new Point(20, 40)))
            .build();

    values.forEach(
        (k, v) -> {
          try {
            String tbl = table + k.rawType().toString().toLowerCase().replace("'", "");
            Column c = Column.create(k.cqlDefinition(), PartitionKey, k);
            dataStore
                .queryBuilder()
                .create()
                .table(keyspace, tbl)
                .column(c)
                .build()
                .execute()
                .join();
            dataStore.queryBuilder().insertInto(keyspace, tbl).value(c, v).build().execute().join();
            // now perform a SELECT * FROM ks.tbl WHERE col = X
            Row row =
                dataStore
                    .queryBuilder()
                    .select()
                    .star()
                    .from(keyspace, tbl)
                    .where(c, EQ, v)
                    .build()
                    .execute()
                    .join()
                    .one();
            assertThat(row).isNotNull();
            assertThat(row.getObject(c.name())).isEqualTo(v);
          } catch (Exception e) {
            fail(e.getMessage(), e);
          }
        });
  }

  @Test
  public void testSecondaryIndexes() throws ExecutionException, InterruptedException {
    // TODO remove this when we figure out how to enable SAI indexes in Cassandra 4
    assumeThat(isCassandra4())
        .as(
            "Disabled because it is currently not possible to enable SAI indexes "
                + "on a Cassandra 4 backend")
        .isFalse();
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Text)
        .column("c", Uuid)
        .column("d", Text)
        .build()
        .execute()
        .join();
    dataStore
        .queryBuilder()
        .create()
        .index("byB")
        .ifNotExists()
        .on(keyspace, table)
        .column("b")
        .build()
        .execute()
        .join();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Text)
                .column("c", Uuid)
                .column("d", Text)
                .secondaryIndex("byB")
                .column("b")
                .build()
                .keyspace(keyspace)
                .table(table));

    dataStore
        .queryBuilder()
        .create()
        .index("byC")
        .ifNotExists()
        .on(keyspace, table)
        .column("c")
        .build()
        .execute()
        .join();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table).toString())
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Text)
                .column("c", Uuid)
                .column("d", Text)
                .secondaryIndex("byB")
                .column("b")
                .secondaryIndex("byC")
                .column("c")
                .build()
                .keyspace(keyspace)
                .table(table)
                .toString());

    String indexClass = "org.apache.cassandra.index.sasi.SASIIndex";
    Map<String, String> indexOptions = new HashMap<>();
    indexOptions.put("mode", "CONTAINS");
    dataStore
        .queryBuilder()
        .create()
        .index("byD")
        .ifNotExists()
        .on(keyspace, table)
        .column("d")
        .custom(indexClass)
        .options(indexOptions)
        .build()
        .execute()
        .join();

    Table actualTable = dataStore.schema().keyspace(keyspace).table(this.table);
    Table expectedTable =
        Schema.build()
            .keyspace(keyspace)
            .table(table)
            .column("a", Int, PartitionKey)
            .column("b", Text)
            .column("c", Uuid)
            .column("d", Text)
            .secondaryIndex("byB")
            .column("b")
            .secondaryIndex("byC")
            .column("c")
            .secondaryIndex("byD")
            .column("d")
            .indexClass(indexClass)
            .indexOptions(indexOptions)
            .build()
            .keyspace(keyspace)
            .table(table);

    // TODO: as indexes are stored in a Map there's no guarantee that the order they
    // printed when calling table#toString() is the same in both expected and actual table
    assertThat(actualTable.indexes().size()).isEqualTo(expectedTable.indexes().size());
    assertThat(actualTable.index("byD").toString())
        .isEqualTo(expectedTable.index("byD").toString());
  }

  @Test
  public void testMaterializedView() {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Text, Clustering)
        .withComment("test-comment1")
        .build()
        .execute()
        .join();
    dataStore
        .queryBuilder()
        .create()
        .materializedView(keyspace, "test_MV")
        .asSelect()
        .column("b", PartitionKey)
        .column("a", Clustering)
        .from(keyspace, table)
        .withComment("test-comment2")
        .build()
        .execute()
        .join();

    MaterializedView mv = dataStore.schema().keyspace(keyspace).materializedView("test_MV");
    assertThat(mv).isNotNull();
    assertThat(mv.name()).isEqualTo("test_MV");
    assertThat(mv.comment()).isEqualTo("test-comment2");
    assertThat(mv.columns()).hasSize(2);
    assertThat(mv.column("b").kind()).isEqualTo(PartitionKey);
    assertThat(mv.column("b").type()).isEqualTo(Text);
    assertThat(mv.column("a").kind()).isEqualTo(Clustering);
    assertThat(mv.column("a").type()).isEqualTo(Int);
  }

  @Disabled("Disabling for now since it currently just hangs")
  @Test
  public void testTupleWithAllSimpleTypes()
      throws UnknownHostException, ExecutionException, InterruptedException {
    Map<Column.ColumnType, Object> typeToValue = getSimpleTypesWithValues();

    List<Column> columns = new ArrayList<>(typeToValue.size());
    typeToValue.forEach(
        (type, value) -> columns.add(Column.create(type.name().toLowerCase(), type)));

    Keyspace ks = createKeyspace();
    ParameterizedType.TupleType tupleType =
        ImmutableTupleType.builder().addAllParameters(typeToValue.keySet()).build();

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("my_tuple", tupleType)
        .build()
        .execute()
        .join();

    Object[] actualValues = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      actualValues[i] = typeToValue.get(columns.get(i).type());
    }

    dataStore
        .queryBuilder()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("my_tuple", tupleType.create(actualValues))
        .build()
        .execute()
        .join();

    java.util.List<Row> rows =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(ks.name(), table)
            .where("x", EQ, 1)
            .build()
            .execute()
            .join()
            .rows();
    assertThat(rows).isNotEmpty();
    Row row = rows.get(0);
    TupleValue tupleValue = row.getTupleValue("my_tuple");
    assertThat(tupleValue).isNotNull();
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      assertThat(tupleValue.get(i, Objects.requireNonNull(column.type()).codec()))
          .isEqualTo(typeToValue.get(column.type()));
    }
  }

  @Test
  public void testUDTWithAllSimpleTypes()
      throws UnknownHostException, ExecutionException, InterruptedException {
    Map<Column.ColumnType, Object> typeToValue = getSimpleTypesWithValues();

    List<Column> columns = new ArrayList<>(typeToValue.size());
    typeToValue.forEach(
        (type, value) -> columns.add(Column.create(type.name().toLowerCase(), type)));

    Keyspace ks = createKeyspace();
    String typeName = "my_type";
    UserDefinedType udtType =
        ImmutableUserDefinedType.builder()
            .name(typeName)
            .keyspace(ks.name())
            .addAllColumns(columns)
            .build();
    dataStore.queryBuilder().create().type(keyspace, udtType).build().execute().join();
    ks = dataStore.schema().keyspace(ks.name());

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("udt", ks.userDefinedType(typeName).frozen())
        .build()
        .execute()
        .join();

    Object[] actualValues = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      actualValues[i] = typeToValue.get(columns.get(i).type());
    }

    dataStore
        .queryBuilder()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("udt", ks.userDefinedType(typeName).create(actualValues))
        .build()
        .execute()
        .join();

    java.util.List<Row> rows =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(ks.name(), table)
            .where("x", EQ, 1)
            .build()
            .execute()
            .join()
            .rows();
    assertThat(rows).isNotEmpty();

    Row row = rows.get(0);
    UdtValue udtValue = row.getUdtValue("udt");
    assertThat(udtValue).isNotNull();
    columns.forEach(
        c ->
            assertThat(
                    udtValue.get(c.name().toLowerCase(), Objects.requireNonNull(c.type()).codec()))
                .isEqualTo(typeToValue.get(c.type())));
  }

  private ImmutableMap<Column.ColumnType, Object> getSimpleTypesWithValues()
      throws UnknownHostException {
    return ImmutableMap.<Column.ColumnType, Object>builder()
        .put(Ascii, "hi")
        .put(Bigint, 23L)
        .put(Blob, ByteBuffer.wrap(new byte[] {2, 3}))
        .put(Boolean, true)
        .put(Date, LocalDate.ofEpochDay(34))
        .put(Decimal, BigDecimal.valueOf(2.3))
        .put(Double, 3.8)
        .put(Duration, CqlDuration.newInstance(2, 3, 5)) // can't be used as PK
        .put(Float, 4.9f)
        .put(Inet, Inet4Address.getByAddress(new byte[] {2, 3, 4, 5}))
        .put(Int, 4)
        .put(Smallint, (short) 7)
        .put(Text, "some text")
        .put(Time, LocalTime.ofSecondOfDay(20))
        .put(Timestamp, Instant.ofEpochSecond(3))
        .put(Timeuuid, timeBased())
        .put(Tinyint, (byte) 4)
        .put(Uuid, random())
        .put(Varint, BigInteger.valueOf(23))
        //                .put(Point, new Point(3.3, 4.4))
        //                .put(Polygon, new Polygon(new Point(30, 10), new Point(10, 20), new
        // Point(20, 40), new Point(40,
        //                        40)))
        //                .put(LineString, new LineString(new Point(30, 10), new Point(40, 40), new
        // Point(20, 40)))
        .build();
  }

  @Disabled("Disabling for now since it currently just hangs")
  @Test
  public void testUDT() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();
    String typeName = "my_type";
    UserDefinedType udtType =
        ImmutableUserDefinedType.builder()
            .name(typeName)
            .keyspace(ks.name())
            .addColumns(
                Column.create("a", Int),
                Column.create("mylist", List.of(Double)),
                Column.create("myset", Set.of(Double)),
                Column.create("mymap", Map.of(Text, Int)),
                Column.create("mytuple", Tuple.of(Text, Tuple.of(Int, Double))))
            .build();
    dataStore.queryBuilder().create().type(keyspace, udtType).build().execute().join();
    ks = dataStore.schema().keyspace(ks.name());
    Column.ColumnType nestedTuple = Tuple.of(Int, Double);
    Column.ColumnType tupleType = Tuple.of(Text, nestedTuple);

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("udt", ks.userDefinedType(typeName).frozen())
        .build()
        .execute()
        .join();

    java.util.List<java.lang.Double> list = Arrays.asList(3.0, 4.5);
    Map<String, Integer> map = ImmutableMap.of("Alice", 3, "Bob", 4);
    Set<Double> set = ImmutableSet.of(3.4, 5.3);
    Object tuple = tupleType.create("Test", nestedTuple.create(2, 3.0));

    dataStore
        .queryBuilder()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("udt", ks.userDefinedType(typeName).create(23, list, set, map, tuple))
        .build()
        .execute()
        .join();

    java.util.List<Row> rows =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(ks.name(), table)
            .where("x", EQ, 1)
            .build()
            .execute()
            .join()
            .rows();
    assertThat(rows).isNotEmpty();

    // TODO: [doug] 2020-07-10, Fri, 16:43 we currently have a mismatch where some things are using
    // UdtValue and others want UDTValue
    //        Row row = rows.get(0);
    //        UDTValue udtValue = row.getUDT("udt");
    //        assertThat(udtValue).isNotNull();
    //        assertThat(udtValue.getInt("a")).isEqualTo(23);
    //        assertThat(udtValue.getList("mylist", Double.class)).isEqualTo(list);
    //        assertThat(udtValue.getMap("mymap", String.class, Integer.class)).isEqualTo(map);
    //        assertThat(udtValue.getSet("myset", Double.class)).isEqualTo(set);
    //        assertThat(udtValue.getTupleValue("mytuple")).isEqualTo(tuple);
  }

  @Disabled("Disabling for now since it currently just hangs")
  @Test
  public void testTupleMismatch() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();
    Column.ColumnType nested = Tuple.of(Text, Double);
    Column.ColumnType tupleType = Tuple.of(Text, nested);
    Column.ColumnType alternativeNested = Tuple.of(Int, Double);
    Column.ColumnType alternativeType = Tuple.of(Text, alternativeNested);

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("tuple", tupleType)
        .build()
        .execute()
        .join();

    dataStore.waitForSchemaAgreement();

    Object tuple = alternativeType.create("Test", alternativeNested.create(4, 3.0));

    dataStore.waitForSchemaAgreement();

    try {
      dataStore
          .queryBuilder()
          .insertInto(ks.name(), table)
          .value("x", 1)
          .value("tuple", tuple)
          .build()
          .execute()
          .join();

      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      assertThat(ex)
          .hasMessage(
              "Wrong value type provided for column 'tuple'. Provided type 'Integer' is not compatible with expected CQL type 'Text' at location 'tuple.frozen<tuple<Text, frozen<tuple<Text, double>>>>[1].frozen<tuple<Text, double>>[0]'.");
    }
  }

  @Test
  public void testOrdinaryTypeMismatch() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("name", Text)
        .build()
        .execute()
        .join();

    Assertions.assertThatThrownBy(
            () ->
                dataStore
                    .queryBuilder()
                    .insertInto(ks.name(), table)
                    .value("x", 1)
                    .value("name", 42)
                    .build()
                    .execute()
                    .join())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value provided for 'name': "
                + "Java value 42 of type 'java.lang.Integer' is not a valid value for CQL type text");
  }

  @Disabled("Disabling for now since it fails with a strange MV schema generated")
  @Test
  public void testMvIndexes() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Text)
        .column("c", Uuid)
        .build()
        .execute()
        .join();

    dataStore
        .queryBuilder()
        .create()
        .materializedView(keyspace, "byB")
        .asSelect()
        .column("b", PartitionKey)
        .column("a", Clustering, DESC)
        .column("c")
        .from(keyspace, table)
        .build()
        .execute()
        .join();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Text)
                .column("c", Uuid)
                .materializedView("byB")
                .column("b", PartitionKey)
                .column("a", Clustering, DESC)
                .column("c")
                .build()
                .keyspace(keyspace)
                .table(table));

    dataStore
        .queryBuilder()
        .create()
        .materializedView(keyspace, "byC")
        .asSelect()
        .column("c", PartitionKey)
        .column("a", Clustering, ASC)
        .column("b")
        .from(keyspace, table)
        .build()
        .execute()
        .join();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Text)
                .column("c", Uuid)
                .materializedView("byB")
                .column("b", PartitionKey)
                .column("a", Clustering, DESC)
                .column("c")
                .materializedView("byC")
                .column("c", PartitionKey)
                .column("a", Clustering, ASC)
                .column("b")
                .build()
                .keyspace(keyspace)
                .table(table));
  }

  @Test
  public void testPagination() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .build()
        .execute()
        .join();
    Set<Integer> knownValues = new HashSet<>();

    // TODO: [doug] 2020-07-10, Fri, 14:48 figure out what storeName should be
    //        int pageSize = storeName.equals("internalWithPaging") ? CUSTOM_PAGE_SIZE :
    // DataStore.DEFAULT_ROWS_PER_PAGE;
    int pageSize = CUSTOM_PAGE_SIZE;

    // fetch size * 2 + 3 rows
    int totalResultSize = (pageSize * 2) + 3;
    for (int i = 1; i <= totalResultSize; i++) {
      knownValues.add(i);
      dataStore.queryBuilder().insertInto(keyspace, table).value("a", i).build().execute().join();
    }

    // results come back unordered
    ResultSet resultSet =
        dataStore.queryBuilder().select().star().from(keyspace, table).build().execute().join();

    Iterator<Row> it = resultSet.iterator();
    iterateOverResults(it, knownValues, pageSize); // page 1
    iterateOverResults(it, knownValues, pageSize); // page 2
    iterateOverResults(it, knownValues, 3); // page 3

    assertThat(it).isExhausted();

    // we should have seen all values
    assertThat(knownValues).isEmpty();
  }

  private void iterateOverResults(Iterator<Row> it, Set<Integer> knownValues, int pageSize) {
    for (int i = 1; i <= pageSize; i++) {
      assertThat(knownValues.remove(it.next().getInt("a"))).isTrue();
    }
  }

  @Test
  public void testINClause() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .build()
        .execute()
        .join();

    for (int i = 0; i < 10; i++) {
      dataStore.queryBuilder().insertInto(keyspace, table).value("a", i).build().execute().join();
    }

    Set<Integer> set = new HashSet<>(Arrays.asList(1, 3, 5, 7));
    ResultSet resultSet =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(keyspace, table)
            .where("a", IN, Arrays.asList(1, 3, 5, 7))
            .build()
            .execute()
            .join();

    resultSet.iterator().forEachRemaining(row -> assertThat(set.remove(row.getInt("a"))).isTrue());
    assertThat(set).isEmpty();
  }

  @Test
  public void testWithUnsetParameter() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Int)
        .build()
        .execute()
        .join();
    Table tbl = dataStore.schema().keyspace(keyspace).table(table);

    dataStore
        .queryBuilder()
        .insertInto(tbl)
        .value("a")
        .value("b")
        .build()
        .execute(23, TypedValue.UNSET)
        .get();

    ResultSet resultSet =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(keyspace, table)
            .where("a", EQ, 23)
            .build()
            .execute()
            .join();

    assertThat(resultSet).isNotEmpty();
  }

  @Test
  public void testDeleteCell() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .build()
        .execute()
        .join();
    dataStore
        .queryBuilder()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .build()
        .execute()
        .join();

    Row row =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(keyspace, table)
            .build()
            .execute()
            .join()
            .one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.getString("name")).isEqualTo("Bob");
    dataStore
        .queryBuilder()
        .update(keyspace, table)
        .value("name", null)
        .where("graph", EQ, true)
        .build()
        .execute()
        .join();

    row =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(keyspace, table)
            .build()
            .execute()
            .join()
            .one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.isNull("name")).isEqualTo(true);
  }

  @Test
  public void testPrepStmtCacheInvalidation() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .build()
        .execute()
        .join();

    dataStore
        .queryBuilder()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .build()
        .execute()
        .join();
    Keyspace ks = dataStore.schema().keyspace(keyspace);
    Table tbl = ks.table(table);

    Row row = dataStore.queryBuilder().select().star().from(tbl).build().execute().join().one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.getString("name")).isEqualTo("Bob");

    // drop the whole graph, which should invalidate the prepared stmt cache.
    // afterwards we should be able to re-create the same schema and insert the same data without a
    // problem
    dataStore.queryBuilder().drop().keyspace(ks.name()).build().execute().join();

    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .build()
        .execute()
        .join();

    // if we wouldn't invalidate the prep stmt cache, then inserting would fail with
    // org.apache.cassandra.exceptions.UnknownTableException: Cannot find table, it may have been
    // dropped
    dataStore
        .queryBuilder()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .build()
        .execute()
        .join();

    ks = dataStore.schema().keyspace(keyspace);
    tbl = ks.table(table);

    row = dataStore.queryBuilder().select().star().from(tbl).build().execute().join().one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.getString("name")).isEqualTo("Bob");
  }

  @Test
  public void testKeyspaceReplicationAndDurableWrites()
      throws ExecutionException, InterruptedException {
    createKeyspace();
    Keyspace ks = dataStore.schema().keyspace(keyspace);
    assertThat(ks.replication())
        .isEqualTo(
            ImmutableMap.of(
                "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));
    assertThat(ks.durableWrites()).isPresent().contains(true);
  }

  @Test
  public void testTableCommentSelect() {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .withComment("This is a table")
        .build()
        .execute()
        .join();
    dataStore.waitForSchemaAgreement();
    Table theTable = dataStore.schema().keyspace(keyspace).table(table);
    assertThat(theTable.comment()).isEqualTo("This is a table");

    dataStore
        .queryBuilder()
        .alter()
        .table(keyspace, table)
        .withComment("This is still a table")
        .build()
        .execute()
        .join();
    dataStore.waitForSchemaAgreement();

    theTable = dataStore.schema().keyspace(keyspace).table(table);
    assertThat(theTable.comment()).isEqualTo("This is still a table");
  }

  @Test
  public void testInsertWithTTL() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .build()
        .execute()
        .join();
    dataStore
        .queryBuilder()
        .insertInto(keyspace, table)
        .value("graph", true)
        .ttl(2)
        .build()
        .execute()
        .join();
  }

  @Test
  public void testUpdateWithTTL() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .build()
        .execute()
        .join();
    dataStore
        .queryBuilder()
        .update(keyspace, table)
        .ttl(2)
        .value("name", "Bif")
        .where("graph", EQ, true)
        .build()
        .execute()
        .join();
  }

  @Test
  public void testCqlExecutionInfo() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();

    dataStore
        .queryBuilder()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .build()
        .execute()
        .join();

    dataStore.queryBuilder().insertInto(ks.name(), table).value("x", 1).build().execute().join();

    ResultSet resultSet =
        dataStore
            .queryBuilder()
            .select()
            .star()
            .from(ks.name(), table)
            .where("x", EQ, 1)
            .build()
            .execute()
            .join();
    java.util.List<Row> rows = resultSet.rows();
    assertThat(rows).isNotEmpty();
    assertThat(rows.size()).isEqualTo(1);
  }

  @Test
  public void testSimpleSelectStmt() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .build()
        .execute()
        .join();

    dataStore.waitForSchemaAgreement();

    ResultSet insert =
        dataStore
            .queryBuilder()
            .insertInto(keyspace, table)
            .value("graph", true)
            .build()
            .execute()
            .join();
    assertThat(insert.waitedForSchemaAgreement()).isFalse();

    ResultSet select =
        dataStore.queryBuilder().select().star().from(keyspace, table).build().execute().join();
    assertThat(select.waitedForSchemaAgreement()).isFalse();

    assertThat(select.hasNoMoreFetchedRows()).isFalse();
    assertThat(select.one().getBoolean("graph")).isTrue();
  }

  @Test
  public void testLimit() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("pk", Int, PartitionKey)
        .column("cc", Int, Clustering)
        .column("val", Int)
        .build()
        .execute()
        .join();

    dataStore.waitForSchemaAgreement();

    BuiltQuery<?> insert =
        dataStore
            .queryBuilder()
            .insertInto(keyspace, table)
            .value("pk")
            .value("cc")
            .value("val")
            .build();

    insert.bind(10, 1, 1).execute().join();
    insert.bind(10, 2, 2).execute().join();
    insert.bind(20, 3, 3).execute().join();

    BuiltQuery<?> select =
        dataStore.queryBuilder().select().column("val").from(keyspace, table).limit().build();

    assertThat(select.bind(1).execute().get().rows()).hasSize(1);
    assertThat(select.bind(2).execute().get().rows()).hasSize(2);
    assertThat(select.bind(10).execute().get().rows()).hasSize(3);

    assertThat(
            dataStore
                .queryBuilder()
                .select()
                .column("val")
                .from(keyspace, table)
                .limit(2) // literal limit
                .build()
                .execute()
                .get()
                .rows())
        .hasSize(2);
  }

  @Test
  public void testPerPartitionLimit() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("pk", Int, PartitionKey)
        .column("cc", Int, Clustering)
        .column("val", Int)
        .build()
        .execute()
        .join();

    dataStore.waitForSchemaAgreement();

    BuiltQuery<?> insert =
        dataStore
            .queryBuilder()
            .insertInto(keyspace, table)
            .value("pk")
            .value("cc")
            .value("val")
            .build();

    insert.bind(10, 1, 1).execute().join();
    insert.bind(10, 2, 2).execute().join();
    insert.bind(10, 3, 3).execute().join();

    insert.bind(20, 4, 4).execute().join();
    insert.bind(20, 5, 5).execute().join();
    insert.bind(20, 6, 6).execute().join();

    BuiltQuery<?> select =
        dataStore
            .queryBuilder()
            .select()
            .column("val")
            .from(keyspace, table)
            .perPartitionLimit()
            .build();

    assertThat(select.bind(1).execute().get().rows()).hasSize(2); // 1 row * 2 partitions
    assertThat(select.bind(2).execute().get().rows()).hasSize(4); // 2 rows * 2 partitions
    assertThat(select.bind(10).execute().get().rows()).hasSize(6); // all data

    assertThat(
            dataStore
                .queryBuilder()
                .select()
                .column("val")
                .from(keyspace, table)
                .perPartitionLimit(2) // literal limit
                .build()
                .execute()
                .get()
                .rows())
        .hasSize(4); // 2 rows * 2 partitions

    assertThat(
            dataStore
                .queryBuilder()
                .select()
                .column("val")
                .from(keyspace, table)
                .perPartitionLimit(3) // literal limit
                .limit(2) // literal limit
                .build()
                .execute()
                .get()
                .rows())
        .hasSize(2); // hard limit for 2 rows

    select =
        dataStore
            .queryBuilder()
            .select()
            .column("val")
            .from(keyspace, table)
            .perPartitionLimit()
            .limit()
            .build();

    assertThat(select.bind(1, 10).execute().get().rows()).hasSize(2); // 1 row * 2 partitions
    assertThat(select.bind(1, 1).execute().get().rows()).hasSize(1); // 1 row hard limit
    assertThat(select.bind(2, 10).execute().get().rows()).hasSize(4); // 2 rows * 2 partitions
    assertThat(select.bind(2, 3).execute().get().rows()).hasSize(3); // 3 rows hard limit
    assertThat(select.bind(10, 10).execute().get().rows()).hasSize(6); // all data
  }

  private Keyspace createKeyspace() {
    dataStore
        .queryBuilder()
        .create()
        .keyspace(keyspace)
        .ifNotExists()
        .withReplication(Replication.simpleStrategy(1))
        .andDurableWrites(true)
        .build()
        .execute()
        .join();

    Keyspace keyspace = dataStore.schema().keyspace(this.keyspace);
    assertThat(keyspace).isNotNull();
    assertThat(keyspace.name()).isEqualTo(this.keyspace);
    assertThat(keyspace.tables()).isEmpty();
    assertThat(keyspace.replication())
        .isEqualTo(
            ImmutableMap.of(
                "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));
    assertThat(keyspace.durableWrites()).isPresent().contains(true);

    dataStore.waitForSchemaAgreement();

    return keyspace;
  }

  private static Result execute(
      Persistence.Connection connection, String query, ByteBuffer... values)
      throws ExecutionException, InterruptedException {
    return connection
        .execute(
            new SimpleStatement(query, Arrays.asList(values)),
            Parameters.defaults(),
            System.nanoTime())
        .get();
  }

  @Test
  public void testWarnings() throws ExecutionException, InterruptedException {
    // This test is admittedly a bit fragile, because the exact warnings that may or may not be
    // raised are not strongly defined. Here, we pick the warning on TTL too large, because it's
    // currently raised by all our persistence implementation, it's easy to trigger, and it feels
    // like it'll probably stay for a while. But if this test start failing on some persistence
    // implementation, we may have to adapt.
    Keyspace ks = createKeyspace();

    Persistence.Connection conn = persistence().newConnection();

    execute(conn, "USE " + ks.cqlName());
    execute(conn, "CREATE TABLE t (k int PRIMARY KEY)");

    conn.waitForSchemaAgreement();

    // TTLs have a hard cap to 20 years, so we can't pass a bigger one that that or it will hard
    // throw rather than warn (and cap the TTL).
    ByteBuffer ttl = Int.codec().encode(20 * 365 * 24 * 60 * 60, ProtocolVersion.DEFAULT);
    Result result = execute(conn, "INSERT INTO t (k) VALUES (0) USING TTL ?", ttl);

    assertThat(result.getWarnings()).hasSize(1);
    assertThat(result.getWarnings().get(0)).contains("exceeds maximum supported expiration");
  }

  @Test
  public void testSchemaAgreementAchievable() {
    assertThat(persistence().isSchemaAgreementAchievable()).isTrue();
  }

  private void setupCustomPagingData() {
    createKeyspace();
    dataStore
        .queryBuilder()
        .create()
        .table(keyspace, table)
        .column("pk", Int, PartitionKey)
        .column("cc", Int, Clustering)
        .column("val", Int)
        .build()
        .execute()
        .join();

    dataStore.waitForSchemaAgreement();

    BuiltQuery<?> insert =
        dataStore
            .queryBuilder()
            .insertInto(keyspace, table)
            .value("pk")
            .value("cc")
            .value("val")
            .build();

    insert.bind(10, 1, 1).execute().join();
    insert.bind(10, 2, 2).execute().join();
    insert.bind(10, 3, 3).execute().join();
    insert.bind(10, 4, 4).execute().join();

    insert.bind(20, 5, 5).execute().join();
    insert.bind(20, 6, 6).execute().join();
    insert.bind(20, 7, 7).execute().join();
    insert.bind(20, 8, 8).execute().join();

    insert.bind(30, 9, 9).execute().join();
    insert.bind(30, 10, 10).execute().join();
    insert.bind(30, 11, 11).execute().join();
    insert.bind(30, 12, 12).execute().join();

    insert.bind(40, 13, 13).execute().join();
    insert.bind(40, 14, 14).execute().join();
    insert.bind(40, 15, 15).execute().join();
    insert.bind(40, 16, 16).execute().join();
  }

  @Test
  public void testPagingStateForNextPartition() throws ExecutionException, InterruptedException {
    setupCustomPagingData();

    // Obtain partition keys in "ring" order
    AbstractBound<?> selectAll =
        dataStore
            .queryBuilder()
            .select()
            .column("pk")
            .column("val")
            .from(keyspace, table)
            .build()
            .bind();

    java.util.List<Row> allRows = dataStore.execute(selectAll).get().rows();
    List<Integer> keys = allRows.stream().map(r -> r.getInt(0)).collect(Collectors.toList());
    List<Integer> values = allRows.stream().map(r -> r.getInt(1)).collect(Collectors.toList());
    int key2 = keys.get(1);
    int val2 = values.get(1);
    int key5 = keys.get(4);
    int val5 = values.get(4);
    int key6 = keys.get(5);
    int val6 = values.get(5);
    int key10 = keys.get(9);
    int val10 = values.get(9);

    AbstractBound<?> select =
        dataStore
            .queryBuilder()
            .select()
            .column("pk")
            .column("val")
            .from(keyspace, table)
            .perPartitionLimit(3)
            .limit(10)
            .build()
            .bind();

    ResultSet rs = dataStore.execute(select, p -> p.toBuilder().pageSize(5).build()).get();
    java.util.List<Row> rows = rs.currentPageRows();
    assertThat(rows).hasSize(5); // full page
    Row row2 = rows.get(1);
    assertThat(row2.getInt(0)).isEqualTo(key2);
    assertThat(row2.getInt(1)).isEqualTo(val2);
    // with 3 row per partition, the last row in page 1 is the second row of the second partition
    Row row6 = rows.get(4);
    assertThat(row6.getInt(0)).isEqualTo(key6);
    assertThat(row6.getInt(1)).isEqualTo(val6);

    ByteBuffer pagingState =
        rs.makePagingState(
            PagingPosition.ofCurrentRow(row2)
                .resumeFrom(ResumeMode.NEXT_PARTITION)
                .remainingRows(7) // not inherited from the query
                .build());

    rs =
        dataStore
            .execute(select, p -> p.toBuilder().pageSize(5).pagingState(pagingState).build())
            .get();
    rows = rs.currentPageRows();
    assertThat(rows).hasSize(5); // full page

    Row row5 = rows.get(0);
    assertThat(row5.getInt(0)).isEqualTo(key5);
    assertThat(row5.getInt(1)).isEqualTo(val5);
    // with 3 row per partition, the last row in page 1 is the second row of the third partition
    Row row10 = rows.get(4);
    assertThat(row10.getInt(0)).isEqualTo(key10);
    assertThat(row10.getInt(1)).isEqualTo(val10);

    // With the limit of 7 rows in the second execution and 5 rows read in the second page,
    // 2 rows remain
    assertThat(rs.rows()).hasSize(2);
  }

  @Test
  public void testPagingStateForNextRow() throws ExecutionException, InterruptedException {
    setupCustomPagingData();

    // Obtain partition keys in "ring" order
    AbstractBound<?> selectAll =
        dataStore
            .queryBuilder()
            .select()
            .column("pk")
            .column("val")
            .from(keyspace, table)
            .build()
            .bind();

    List<Row> allRows = dataStore.execute(selectAll).get().rows();
    List<Integer> keys = allRows.stream().map(r -> r.getInt(0)).collect(Collectors.toList());
    List<Integer> values = allRows.stream().map(r -> r.getInt(1)).collect(Collectors.toList());
    int key2 = keys.get(1);
    int val2 = values.get(1);
    int key3 = keys.get(2);
    int val3 = values.get(2);
    int key6 = keys.get(5);
    int val6 = values.get(5);
    int key8 = keys.get(8);
    int val8 = values.get(8);

    AbstractBound<?> select =
        dataStore
            .queryBuilder()
            .select()
            .column("pk")
            .column("cc")
            .column("val")
            .from(keyspace, table)
            .perPartitionLimit(3)
            .limit(10)
            .build()
            .bind();

    ResultSet rs = dataStore.execute(select, p -> p.toBuilder().pageSize(5).build()).get();
    List<Row> rows = rs.currentPageRows();
    assertThat(rows).hasSize(5); // full page
    Row row2 = rows.get(1);
    assertThat(row2.getInt(0)).isEqualTo(key2);
    assertThat(row2.getInt(1)).isEqualTo(val2);
    // with 3 row per partition, the last row in page 1 is the second row of the second partition
    Row row6 = rows.get(4);
    assertThat(row6.getInt(0)).isEqualTo(key6);
    assertThat(row6.getInt(1)).isEqualTo(val6);

    ByteBuffer pagingState =
        rs.makePagingState(
            PagingPosition.ofCurrentRow(row2)
                .resumeFrom(ResumeMode.NEXT_ROW)
                .remainingRows(7) // not inherited from the query
                .remainingRowsInPartition(1) // not inherited from the query
                .build());

    rs =
        dataStore
            .execute(select, p -> p.toBuilder().pageSize(5).pagingState(pagingState).build())
            .get();
    rows = rs.currentPageRows();
    assertThat(rows).hasSize(5); // full page

    Row row3 = rows.get(0);
    assertThat(row3.getInt(0)).isEqualTo(key3);
    assertThat(row3.getInt(1)).isEqualTo(val3);
    // with 1 row remaining in first partition, the last row in page 1 is the third row of the
    // second partition
    Row row8 = rows.get(4);
    assertThat(row8.getInt(0)).isEqualTo(key8);
    assertThat(row8.getInt(1)).isEqualTo(val8);

    // With the limit of 7 rows in the second execution and 5 rows read in the second page,
    // 2 rows remain
    assertThat(rs.rows()).hasSize(2);
  }

  private void assertEq(Row r1, Row r2, RowDecorator... decorators) {
    for (RowDecorator dec1 : decorators) {
      for (RowDecorator dec2 : decorators) {
        assertThat(dec1.decoratePartitionKey(r1)).isEqualTo(dec2.decoratePartitionKey(r2));
        assertThat(dec1.decoratePartitionKey(r2)).isEqualTo(dec2.decoratePartitionKey(r1));
      }
    }

    assertGtEq(r1, r2, decorators);
  }

  private void assertGtEq(Row r1, Row r2, RowDecorator... decorators) {
    for (RowDecorator dec1 : decorators) {
      ComparableKey<?> k1 = dec1.decoratePartitionKey(r1);
      ComparableKey<?> k2 = dec1.decoratePartitionKey(r1);
      int i = k1.compareTo(k2);
      for (RowDecorator dec2 : decorators) {
        assertThat(dec1.decoratePartitionKey(r1))
            .isGreaterThanOrEqualTo(dec2.decoratePartitionKey(r2));
        assertThat(dec1.decoratePartitionKey(r2))
            .isLessThanOrEqualTo(dec2.decoratePartitionKey(r1));
      }
    }
  }

  private void assertGt(Row r1, Row r2, RowDecorator... decorators) {
    for (RowDecorator dec1 : decorators) {
      for (RowDecorator dec2 : decorators) {
        assertThat(dec1.decoratePartitionKey(r1)).isGreaterThan(dec2.decoratePartitionKey(r2));
        assertThat(dec1.decoratePartitionKey(r2)).isLessThan(dec2.decoratePartitionKey(r1));
      }
    }
  }

  @Test
  public void testRowDecorator() throws ExecutionException, InterruptedException {
    setupCustomPagingData();

    // Obtain partition keys in "ring" order
    AbstractBound<?> selectAll =
        dataStore
            .queryBuilder()
            .select()
            .column("pk")
            .column("val")
            .from(keyspace, table)
            .build()
            .bind();

    ResultSet rs1 = dataStore.execute(selectAll).get();
    Iterator<Row> it1 = rs1.iterator();
    RowDecorator dec1 = rs1.makeRowDecorator();

    ResultSet rs2 = dataStore.execute(selectAll).get();
    RowDecorator dec2 = rs2.makeRowDecorator();
    Iterator<Row> it2 = rs2.iterator();

    Row first = null;
    Row last = null;
    Row p1 = null;
    while (it1.hasNext()) {
      assertThat(it2.hasNext()).isTrue();

      Row r1 = it1.next();
      Row r2 = it2.next();

      first = first == null ? r1 : first;
      last = r1;

      assertEq(r1, r2, dec1, dec2);

      if (p1 == null) {
        p1 = r1;
      }

      assertGtEq(r1, p1, dec1, dec2);
    }

    assertThat(it2.hasNext()).isFalse();

    assertGt(last, first, dec1, dec2);
  }

  private boolean isCassandra4() {
    return !backend.isDse()
        && Version.parse(backend.clusterVersion()).nextStable().compareTo(Version.V4_0_0) >= 0;
  }
}
