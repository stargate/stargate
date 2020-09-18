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
import static io.stargate.db.datastore.query.WhereCondition.Predicate.Eq;
import static io.stargate.db.datastore.query.WhereCondition.Predicate.In;
import static io.stargate.db.datastore.schema.Column.Kind.Clustering;
import static io.stargate.db.datastore.schema.Column.Kind.PartitionKey;
import static io.stargate.db.datastore.schema.Column.Kind.Static;
import static io.stargate.db.datastore.schema.Column.Order.Asc;
import static io.stargate.db.datastore.schema.Column.Order.Desc;
import static io.stargate.db.datastore.schema.Column.Type.*;
import static io.stargate.db.datastore.schema.Column.Type.Boolean;
import static io.stargate.db.datastore.schema.Column.Type.Double;
import static io.stargate.db.datastore.schema.Column.Type.Float;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ExecutionInfo;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.Parameter;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.ImmutableTupleType;
import io.stargate.db.datastore.schema.ImmutableUserDefinedType;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.ParameterizedType;
import io.stargate.db.datastore.schema.Schema;
import io.stargate.db.datastore.schema.Table;
import io.stargate.db.datastore.schema.UserDefinedType;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.NotThreadSafe;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.osgi.framework.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@NotThreadSafe
public class PersistenceTest extends BaseOsgiIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(PersistenceTest.class);

  @Rule public TestName name = new TestName();

  private DataStore dataStore;
  private String table;
  private String keyspace;

  private static final int CUSTOM_PAGE_SIZE = 50;

  @Before
  public void setup() throws InvalidSyntaxException {
    Persistence persistence = getOsgiService("io.stargate.db.Persistence", Persistence.class);
    ClientState clientState = persistence.newClientState("");
    QueryState queryState = persistence.newQueryState(clientState);
    dataStore = persistence.newDataStore(queryState, null);
    logger.info("{} {} {}", clientState, queryState, dataStore);

    String testName = name.getMethodName();
    testName = testName.substring(0, testName.indexOf("["));
    keyspace = "ks_" + testName;
    table = testName;
  }

  @Test
  public void querySystemTables() throws ExecutionException, InterruptedException {
    Future<ResultSet> rs =
        dataStore
            .query()
            .select()
            .column("cluster_name")
            .column("data_center")
            .from("system", "local")
            .future();
    Row row = rs.get().one();

    logger.info(String.valueOf(row));
    assertThat(row).isNotNull();
    assertThat(row.columns().get(0).name()).isEqualTo("cluster_name");
    assertThat(row.columns().get(1).name()).isEqualTo("data_center");
    assertThat(row.getString("cluster_name")).isEqualTo("Test Cluster");
    assertThat(row.getString("data_center")).isEqualTo(datacenter);

    rs = dataStore.query().select().column("data_center").from("system", "peers").future();
    row = rs.get().one();

    logger.info(String.valueOf(row));
    assertThat(row).isNotNull();
    assertThat(row.columns().get(0).name()).isEqualTo("data_center");
  }

  @Test
  public void testKeyspace() throws ExecutionException, InterruptedException {
    createKeyspace();
  }

  @Test
  public void testAlterAndDrop() throws ExecutionException, InterruptedException {
    createKeyspace();
    ResultSet createTable =
        dataStore
            .query()
            .create()
            .table(keyspace, table)
            .column("created", Boolean, PartitionKey)
            .execute();

    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    // assertThat(createTable.waitedForSchemaAgreement()).isTrue();
    Table t = dataStore.schema().keyspace(keyspace).table(table);
    assertThat(t)
        .isEqualToComparingFieldByField(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("created", Boolean, PartitionKey)
                .build()
                .keyspace(keyspace)
                .table(table));

    ResultSet alterTable =
        dataStore.query().alter().table(keyspace, table).addColumn("added", Boolean).execute();
    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    //        assertThat(alterTable.waitedForSchemaAgreement()).isTrue();
    assertThat(dataStore.schema().keyspace(keyspace).table(table))
        .isEqualToComparingFieldByField(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("created", Boolean, PartitionKey)
                .column("added", Boolean)
                .build()
                .keyspace(keyspace)
                .table(table));

    ResultSet dropTable = dataStore.query().drop().table(keyspace, table).execute();
    // TODO: [doug] 2020-07-10, Fri, 16:50 More schemaAgreement to revisit
    //        assertThat(dropTable.waitedForSchemaAgreement()).isTrue();
    assertThat(dataStore.schema().keyspace(keyspace).table(table)).isNull();
  }

  @Test
  public void testColumnKinds() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("PK1", Varchar, PartitionKey)
        .column("PK2", Varchar, PartitionKey)
        .column("CC1", Varchar, Clustering, Asc)
        .column("CC2", Varchar, Clustering, Desc)
        .column("R1", Varchar)
        .column("R2", Varchar)
        .column("S1", Varchar, Static)
        .column("S2", Varchar, Static)
        .execute();
    assertThat(dataStore.schema().keyspace(keyspace).table(table))
        .isEqualToComparingFieldByField(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("PK1", Varchar, PartitionKey)
                .column("PK2", Varchar, PartitionKey)
                .column("CC1", Varchar, Clustering, Asc)
                .column("CC2", Varchar, Clustering, Desc)
                .column("R1", Varchar)
                .column("R2", Varchar)
                .column("S1", Varchar, Static)
                .column("S2", Varchar, Static)
                .build()
                .keyspace(keyspace)
                .table(table));
  }

  @Ignore("Disabling for now since it currently just hangs")
  @Test
  public void testInsertingAndReadingDifferentTypes() throws Exception {
    Keyspace ks = createKeyspace();

    Column.ColumnType nestedTuple = Tuple.of(Int, Double);
    Column.ColumnType tupleType = Tuple.of(Varchar, nestedTuple);
    Column.ColumnType nestedDuration = Tuple.of(Int, Duration);
    Column.ColumnType tupleTypeWithDuration = Tuple.of(Varchar, nestedDuration);
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
    // Column.create("c", Varchar)).build();

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
            .add(Pair.with(Map.of(Varchar, Int), ImmutableMap.of("Alice", 3, "Bob", 4)))
            .add(Pair.with(Set.of(Double), ImmutableSet.of(3.4, 5.3)))
            .add(
                Pair.with(
                    List.of(Duration),
                    Arrays.asList(
                        CqlDuration.newInstance(2, 3, 5), CqlDuration.newInstance(2, 3, 6))))
            .add(
                Pair.with(
                    Map.of(Varchar, Duration),
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
            .add(Pair.with(Varchar, "Hi"))
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
            .add(Pair.with(Map.of(Varchar, Int).frozen(), ImmutableMap.of("Alice", 3, "Bob", 4)))
            .add(Pair.with(Set.of(Double).frozen(), ImmutableSet.of(3.4, 5.3)))
            .build();

    Supplier<Stream<Column.ColumnType>> distinctColumnTypesStream =
        () -> values.stream().map(Pair::getValue0).distinct();

    // TODO: [doug] 2020-07-13, Mon, 9:27 Having trouble with UDTs, removing for now
    //        dataStore.query().create().type(keyspace, udtType).ifNotExists().execute();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("PK", Text, PartitionKey)
        .column(
            distinctColumnTypesStream
                .get()
                .map(columnType -> Column.create(columnName(columnType), columnType))
                .collect(Collectors.toList()))
        .execute();

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
          .query()
          .insertInto(keyspace, table)
          .value("PK", "PK" + i)
          .value(Value.create(columnName(pair.getValue0()), pair.getValue1()))
          .execute();

      Row row =
          dataStore
              .query()
              .select()
              .star()
              .from(keyspace, table)
              .where("PK", Eq, "PK" + i)
              .execute()
              .one();

      assertThat(row.getValue(columnName(pair.getValue0()))).isEqualTo(pair.getValue1());
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
            .put(Varchar, "some varchar")
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
            dataStore.query().create().table(keyspace, tbl).column(c).execute();
            dataStore.query().insertInto(keyspace, tbl).value(c, v).execute();
            // now perform a SELECT * FROM ks.tbl WHERE col = X
            Row row =
                dataStore
                    .query()
                    .select()
                    .star()
                    .from(keyspace, tbl)
                    .where(c, Eq, v)
                    .execute()
                    .one();
            assertThat(row).isNotNull();
            assertThat(row.getValue(c)).isEqualTo(v);
          } catch (Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void testSecondaryIndexes() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Varchar)
        .column("c", Uuid)
        .execute();
    dataStore.query().create().index("byB").ifNotExists().on(keyspace, table).column("b").execute();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualToComparingFieldByField(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Varchar)
                .column("c", Uuid)
                .secondaryIndex("byB")
                .column("b")
                .build()
                .keyspace(keyspace)
                .table(table));

    dataStore.query().create().index("byC").ifNotExists().on(keyspace, table).column("c").execute();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table).toString())
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Varchar)
                .column("c", Uuid)
                .secondaryIndex("byB")
                .column("b")
                .secondaryIndex("byC")
                .column("c")
                .build()
                .keyspace(keyspace)
                .table(table)
                .toString());
  }

  @Ignore("Disabling for now since it currently just hangs")
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
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("my_tuple", tupleType)
        .execute();

    Object[] actualValues = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      actualValues[i] = typeToValue.get(columns.get(i).type());
    }

    dataStore
        .query()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("my_tuple", tupleType.create(actualValues))
        .execute();

    java.util.List<Row> rows =
        dataStore.query().select().star().from(ks.name(), table).where("x", Eq, 1).execute().rows();
    assertThat(rows).isNotEmpty();
    Row row = rows.get(0);
    TupleValue tupleValue = row.getTuple("my_tuple");
    assertThat(tupleValue).isNotNull();
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      assertThat(tupleValue.get(i, Objects.requireNonNull(column.type()).codec()))
          .isEqualTo(typeToValue.get(column.type()));
    }
  }

  @Ignore("Disabling UDT related tests for now")
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
    dataStore.query().create().type(keyspace, udtType).execute();
    ks = dataStore.schema().keyspace(ks.name());

    dataStore
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("udt", ks.userDefinedType(typeName).frozen())
        .execute();

    Object[] actualValues = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      actualValues[i] = typeToValue.get(columns.get(i).type());
    }

    dataStore
        .query()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("udt", ks.userDefinedType(typeName).create(actualValues))
        .execute();

    java.util.List<Row> rows =
        dataStore.query().select().star().from(ks.name(), table).where("x", Eq, 1).execute().rows();
    assertThat(rows).isNotEmpty();
    // TODO: [doug] 2020-07-10, Fri, 16:43 we currently have a mismatch where some things are using
    // UdtValue and others want UDTValue
    //        Row row = rows.get(0);
    //        UDTValue udtValue = row.getUDT("udt");
    //        assertThat(udtValue).isNotNull();
    //        columns.forEach(c -> assertThat(udtValue.get(c.name().toLowerCase(),
    // Objects.requireNonNull(c.type()).codec()))
    //                .isEqualTo(typeToValue.get(c.type())));
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
        .put(Varchar, "some varchar")
        .put(Varint, BigInteger.valueOf(23))
        //                .put(Point, new Point(3.3, 4.4))
        //                .put(Polygon, new Polygon(new Point(30, 10), new Point(10, 20), new
        // Point(20, 40), new Point(40,
        //                        40)))
        //                .put(LineString, new LineString(new Point(30, 10), new Point(40, 40), new
        // Point(20, 40)))
        .build();
  }

  @Ignore("Disabling for now since it currently just hangs")
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
                Column.create("mymap", Map.of(Varchar, Int)),
                Column.create("mytuple", Tuple.of(Varchar, Tuple.of(Int, Double))))
            .build();
    dataStore.query().create().type(keyspace, udtType).execute();
    ks = dataStore.schema().keyspace(ks.name());
    Column.ColumnType nestedTuple = Tuple.of(Int, Double);
    Column.ColumnType tupleType = Tuple.of(Varchar, nestedTuple);

    dataStore
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("udt", ks.userDefinedType(typeName).frozen())
        .execute();

    java.util.List<java.lang.Double> list = Arrays.asList(3.0, 4.5);
    Map<String, Integer> map = ImmutableMap.of("Alice", 3, "Bob", 4);
    Set<Double> set = ImmutableSet.of(3.4, 5.3);
    Object tuple = tupleType.create("Test", nestedTuple.create(2, 3.0));

    dataStore
        .query()
        .insertInto(ks.name(), table)
        .value("x", 1)
        .value("udt", ks.userDefinedType(typeName).create(23, list, set, map, tuple))
        .execute();

    java.util.List<Row> rows =
        dataStore.query().select().star().from(ks.name(), table).where("x", Eq, 1).execute().rows();
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

  @Ignore("Disabling for now since it currently just hangs")
  @Test
  public void testTupleMismatch() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();
    Column.ColumnType nested = Tuple.of(Text, Double);
    Column.ColumnType tupleType = Tuple.of(Varchar, nested);
    Column.ColumnType alternativeNested = Tuple.of(Int, Double);
    Column.ColumnType alternativeType = Tuple.of(Varchar, alternativeNested);

    dataStore
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("tuple", tupleType)
        .execute();

    dataStore.waitForSchemaAgreement();

    Object tuple = alternativeType.create("Test", alternativeNested.create(4, 3.0));

    dataStore.waitForSchemaAgreement();

    try {
      dataStore.query().insertInto(ks.name(), table).value("x", 1).value("tuple", tuple).execute();

      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      assertThat(ex)
          .hasMessage(
              "Wrong value type provided for column 'tuple'. Provided type 'Integer' is not compatible with expected CQL type 'varchar' at location 'tuple.frozen<tuple<varchar, frozen<tuple<varchar, double>>>>[1].frozen<tuple<varchar, double>>[0]'.");
    }
  }

  @Test
  public void testOrdinaryTypeMismatch() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();

    dataStore
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .column("name", Text)
        .execute();

    try {
      dataStore.query().insertInto(ks.name(), table).value("x", 1).value("name", 42).execute();

      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      assertThat(ex)
          .hasMessage(
              "Wrong value type provided for column 'name'. Provided type 'Integer' is not compatible with expected CQL type 'varchar'.");
    }
  }

  @Ignore("Disabling for now since it fails with a strange MV schema generated")
  @Test
  public void testMvIndexes() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Varchar)
        .column("c", Uuid)
        .execute();

    dataStore
        .query()
        .create()
        .materializedView(keyspace, "byB")
        .asSelect()
        .star()
        .column("b", PartitionKey)
        .column("a", Clustering, Desc)
        .from(keyspace, table)
        .execute();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Varchar)
                .column("c", Uuid)
                .materializedView("byB")
                .column("b", PartitionKey)
                .column("a", Clustering, Desc)
                .column("c")
                .build()
                .keyspace(keyspace)
                .table(table));

    dataStore
        .query()
        .create()
        .materializedView(keyspace, "byC")
        .asSelect()
        .star()
        .column("c", PartitionKey)
        .column("a", Clustering, Asc)
        .from(keyspace, table)
        .execute();

    assertThat(dataStore.schema().keyspace(keyspace).table(this.table))
        .isEqualTo(
            Schema.build()
                .keyspace(keyspace)
                .table(table)
                .column("a", Int, PartitionKey)
                .column("b", Varchar)
                .column("c", Uuid)
                .materializedView("byB")
                .column("b", PartitionKey)
                .column("a", Clustering, Desc)
                .column("c")
                .materializedView("byC")
                .column("c", PartitionKey)
                .column("a", Clustering, Asc)
                .column("b")
                .build()
                .keyspace(keyspace)
                .table(table));
  }

  @Test
  public void testPagination() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore.query().create().table(keyspace, table).column("a", Int, PartitionKey).execute();
    Set<Integer> knownValues = new HashSet<>();

    // TODO: [doug] 2020-07-10, Fri, 14:48 figure out what storeName should be
    //        int pageSize = storeName.equals("internalWithPaging") ? CUSTOM_PAGE_SIZE :
    // DataStore.DEFAULT_ROWS_PER_PAGE;
    int pageSize = CUSTOM_PAGE_SIZE;

    // fetch size * 2 + 3 rows
    int totalResultSize = (pageSize * 2) + 3;
    for (int i = 1; i <= totalResultSize; i++) {
      knownValues.add(i);
      dataStore.query().insertInto(keyspace, table).value("a", i).execute();
    }

    // results come back unordered
    ResultSet resultSet = dataStore.query().select().star().from(keyspace, table).execute();

    Iterator<Row> it = resultSet.iterator();
    iterateOverResults(resultSet, it, knownValues, pageSize, totalResultSize); // page 1
    iterateOverResults(resultSet, it, knownValues, pageSize, totalResultSize - pageSize); // page 2
    iterateOverResults(resultSet, it, knownValues, 3, totalResultSize - (pageSize * 2)); // page 3

    if (dataStore.getClass().getSimpleName().equalsIgnoreCase("InternalDataStore")) {
      // page should be exhausted at the end
      assertThat(resultSet.size()).isEqualTo(0);
    }

    // we should have seen all values
    assertThat(knownValues).isEmpty();
  }

  private void iterateOverResults(
      ResultSet resultSet,
      Iterator<Row> it,
      Set<Integer> knownValues,
      int pageSize,
      int totalResultSize) {
    for (int i = 1; i <= pageSize; i++) {
      assertThat(knownValues.remove(it.next().getInt("a"))).isTrue();
      // for internal paging the size is totalSize - elementsAlreadyFetched
      assertThat(resultSet.size()).isEqualTo(totalResultSize - i);
    }

    // for internal paging it will be totalSize - elementsAlreadyFetched
    assertThat(resultSet.size()).isEqualTo(totalResultSize - pageSize);
  }

  @Test
  public void testINClause() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore.query().create().table(keyspace, table).column("a", Int, PartitionKey).execute();

    for (int i = 0; i < 10; i++) {
      dataStore.query().insertInto(keyspace, table).value("a", i).execute();
    }

    Set<Integer> set = new HashSet<>(Arrays.asList(1, 3, 5, 7));
    ResultSet resultSet =
        dataStore
            .query()
            .select()
            .star()
            .from(keyspace, table)
            .where("a", In, Arrays.asList(1, 3, 5, 7))
            .execute();

    resultSet.iterator().forEachRemaining(row -> assertThat(set.remove(row.getInt("a"))).isTrue());
    assertThat(set).isEmpty();
  }

  @Test
  public void testWithUnsetParameter() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("a", Int, PartitionKey)
        .column("b", Int)
        .execute();
    Table tbl = dataStore.schema().keyspace(keyspace).table(table);

    dataStore
        .query(
            String.format("insert into %s.%s (a,b) values (?,?)", tbl.cqlKeyspace(), tbl.cqlName()),
            23,
            Parameter.UNSET)
        .get();

    ResultSet resultSet =
        dataStore.query().select().star().from(keyspace, table).where("a", Eq, 23).execute();

    assertThat(resultSet).isNotEmpty();
  }

  @Test
  public void testDeleteCell() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .execute();
    dataStore
        .query()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .execute();

    Row row = dataStore.query().select().star().from(keyspace, table).execute().one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.getString("name")).isEqualTo("Bob");
    dataStore
        .query()
        .update(keyspace, table)
        .value("name", null)
        .where("graph", Eq, true)
        .execute();

    row = dataStore.query().select().star().from(keyspace, table).execute().one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.has("name")).isEqualTo(false);
  }

  @Test
  public void testPrepStmtCacheInvalidation() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .execute();

    dataStore
        .query()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .execute();
    Keyspace ks = dataStore.schema().keyspace(keyspace);
    Table tbl = ks.table(table);

    Row row = dataStore.query().select().star().from(ks, tbl).execute().one();
    assertThat(row.getBoolean("graph")).isTrue();
    assertThat(row.getString("name")).isEqualTo("Bob");

    // drop the whole graph, which should invalidate the prepared stmt cache.
    // afterwards we should be able to re-create the same schema and insert the same data without a
    // problem
    dataStore.query().drop().keyspace(ks.name()).execute();

    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .execute();

    // if we wouldn't invalidate the prep stmt cache, then inserting would fail with
    // org.apache.cassandra.exceptions.UnknownTableException: Cannot find table, it may have been
    // dropped
    dataStore
        .query()
        .insertInto(keyspace, table)
        .value("graph", true)
        .value("name", "Bob")
        .execute();

    ks = dataStore.schema().keyspace(keyspace);
    tbl = ks.table(table);

    row = dataStore.query().select().star().from(ks, tbl).execute().one();
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
  public void testInsertWithTTL() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .execute();
    dataStore.query().insertInto(keyspace, table).value("graph", true).ttl(2).execute();
  }

  @Test
  public void testUpdateWithTTL() throws ExecutionException, InterruptedException {
    createKeyspace();
    dataStore
        .query()
        .create()
        .table(keyspace, table)
        .column("graph", Boolean, PartitionKey)
        .column("name", Column.Type.Text)
        .execute();
    dataStore
        .query()
        .update(keyspace, table)
        .ttl(2)
        .value("name", "Bif")
        .where("graph", Eq, true)
        .execute();
  }

  @Test
  public void testCqlExecutionInfo() throws ExecutionException, InterruptedException {
    Keyspace ks = createKeyspace();

    dataStore
        .query()
        .create()
        .table(ks.name(), table)
        .ifNotExists()
        .column("x", Int, PartitionKey)
        .execute();

    dataStore.query().insertInto(ks.name(), table).value("x", 1).execute();

    ResultSet resultSet =
        dataStore.query().select().star().from(ks.name(), table).where("x", Eq, 1).execute();
    java.util.List<Row> rows = resultSet.rows();
    assertThat(rows).isNotEmpty();
    assertThat(rows.size()).isEqualTo(1);

    ExecutionInfo executionInfo = resultSet.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    assertThat(executionInfo.count()).isEqualTo(1);
    assertThat(executionInfo.durationNanos()).isGreaterThan(1);
    Table tbl = dataStore.schema().keyspace(ks.name()).table(this.table);
    assertThat(executionInfo.preparedCQL())
        .isEqualTo(String.format("SELECT * FROM %s.%s WHERE x = ?", ks.cqlName(), tbl.cqlName()));
  }

  @Test
  public void testSimpleSelectStmt() throws ExecutionException, InterruptedException {
    createKeyspace();
    ResultSet createTable =
        dataStore
            .query()
            .create()
            .table(keyspace, table)
            .column("graph", Boolean, PartitionKey)
            .execute();

    dataStore.waitForSchemaAgreement();

    ResultSet insert = dataStore.query().insertInto(keyspace, table).value("graph", true).execute();
    assertThat(insert.waitedForSchemaAgreement()).isFalse();

    ResultSet select = dataStore.query().select().star().from(keyspace, table).execute();
    assertThat(select.waitedForSchemaAgreement()).isFalse();

    assertThat(select.isEmpty()).isFalse();
    assertThat(select.one().getBoolean("graph")).isTrue();
  }

  private Keyspace createKeyspace() throws ExecutionException, InterruptedException {
    ResultSet result =
        dataStore
            .query()
            .create()
            .keyspace(keyspace)
            .ifNotExists()
            .withReplication("{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
            .andDurableWrites(true)
            .execute();

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
}
