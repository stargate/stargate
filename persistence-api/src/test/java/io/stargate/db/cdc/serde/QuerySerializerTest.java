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
package io.stargate.db.cdc.serde;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.stargate.db.cdc.api.MutationEventType;
import io.stargate.db.cdc.serde.avro.SchemaConstants;
import io.stargate.db.query.*;
import io.stargate.db.schema.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

class QuerySerializerTest {

  public static final Table EMPTY_TABLE =
      Table.create("ks", "table", Collections.emptyList(), Collections.emptyList());

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeBoundDMLQueryTable() throws IOException {
    // given
    BoundDMLQuery boundDMLQuery =
        createBoundDMLQuery(
            Table.create(
                "ks_1",
                "table_1",
                Arrays.asList(
                    Column.create(
                        "pk_1", Column.Kind.PartitionKey, Column.Type.Ascii, Column.Order.ASC),
                    Column.create("col_1", Column.Kind.Regular, Column.Type.Int),
                    Column.create("col_2", Column.Kind.Regular),
                    Column.create("col_3", Column.Type.Counter)),
                Collections.emptyList()));

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    assertThat(byteBuffer.array().length).isGreaterThan(0);
    GenericRecord result = toGenericRecord(byteBuffer);
    // validate table
    GenericRecord table = (GenericRecord) result.get(SchemaConstants.MUTATION_EVENT_TABLE);
    assertThat(table.get(SchemaConstants.TABLE_KEYSPACE).toString()).isEqualTo("ks_1");
    assertThat(table.get(SchemaConstants.TABLE_NAME).toString()).isEqualTo("table_1");

    // validate columns
    GenericData.Array<GenericData.Record> columns =
        (GenericData.Array) table.get(SchemaConstants.TABLE_COLUMNS);
    assertThat(columns.size()).isEqualTo(4);
    validateColumn(
        columns.get(0), Column.Type.Ascii.cqlDefinition(), "ASC", "PartitionKey", "pk_1");
    validateColumn(columns.get(1), Column.Type.Int.cqlDefinition(), null, "Regular", "col_1");
    validateColumn(columns.get(2), null, null, "Regular", "col_2");
    validateColumn(columns.get(3), Column.Type.Counter.cqlDefinition(), null, "Regular", "col_3");
  }

  @Test
  public void shouldSerializeBoundDMLQueryTimestampAndTTL() throws IOException {
    // given
    BoundDMLQuery boundDMLQuery = createBoundDMLQuery(OptionalInt.of(100), OptionalLong.of(10000L));

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TTL)).isEqualTo(100);
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TIMESTAMP)).isEqualTo(10000L);
  }

  @Test
  public void shouldSerializeBoundDMLQueryTimestampAndTTLNull() throws IOException {
    // given
    BoundDMLQuery boundDMLQuery = createBoundDMLQuery(OptionalInt.empty(), OptionalLong.empty());

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TTL)).isNull();
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TIMESTAMP)).isNull();
  }

  @Test
  public void shouldSerializeBoundDMLQueryUpdateType() throws IOException {
    // given
    BoundDMLQuery boundDMLQuery = createBoundDMLQuery(QueryType.UPDATE);

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TYPE).toString())
        .isEqualTo(MutationEventType.UPDATE.name());
  }

  @Test
  public void shouldSerializeBoundDMLQueryDeleteType() throws IOException {
    // given
    BoundDMLQuery boundDMLQuery = createBoundDMLQuery(QueryType.DELETE);

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    assertThat(result.get(SchemaConstants.MUTATION_EVENT_TYPE).toString())
        .isEqualTo(MutationEventType.DELETE.name());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializePartitionKeys() throws IOException {
    // given
    Column pk1 = Column.create("pk_1", Column.Kind.PartitionKey, Column.Type.Int);
    Column pk2 = Column.create("pk_2", Column.Kind.PartitionKey, Column.Type.Boolean);
    Table table =
        Table.create(
            "ks_1",
            "table_1",
            Arrays.asList(
                pk1,
                pk2,
                Column.create("ck_1", Column.Kind.Clustering, Column.Type.Ascii),
                Column.create("col_1", Column.Kind.Regular, Column.Type.Int),
                Column.create("static", Column.Kind.Static, Column.Type.Int)),
            Collections.emptyList());
    BoundDMLQuery boundDMLQuery =
        createBoundDMLQueryPks(
            table,
            Arrays.asList(
                new PrimaryKey(
                    table,
                    Arrays.asList(
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), pk1.name(), pk1.type(), 1),
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), pk2.name(), pk2.type(), true),
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), "ck_1", Column.Type.Ascii, "v")))));

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    GenericData.Array<GenericData.Record> partitionKeys =
        (GenericData.Array) result.get(SchemaConstants.MUTATION_EVENT_PARTITION_KEYS);
    assertThat(partitionKeys.size()).isEqualTo(2);
    GenericData.Record pkRecord1 = partitionKeys.get(0);
    GenericData.Record pkRecord2 = partitionKeys.get(1);
    // validate PKs columns
    validateColumn(
        (GenericData.Record) pkRecord1.get(SchemaConstants.CELL_VALUE_COLUMN),
        pk1.type().cqlDefinition(),
        null,
        pk1.kind().name(),
        pk1.name());
    validateColumn(
        (GenericData.Record) pkRecord2.get(SchemaConstants.CELL_VALUE_COLUMN),
        pk2.type().cqlDefinition(),
        null,
        pk2.kind().name(),
        pk2.name());
    // validate if byte buffers carry correct data
    validateColumnValue(
        (ByteBuffer) pkRecord1.get(SchemaConstants.CELL_VALUE_VALUE),
        pk1.type().cqlDefinition(),
        1,
        table);
    validateColumnValue(
        (ByteBuffer) pkRecord2.get(SchemaConstants.CELL_VALUE_VALUE),
        pk2.type().cqlDefinition(),
        true,
        table);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeClusteringKeys() throws IOException {
    // given
    Column ck1 = Column.create("ck_1", Column.Kind.Clustering, Column.Type.Ascii);
    Table table =
        Table.create(
            "ks_1",
            "table_1",
            Arrays.asList(
                Column.create("pk_1", Column.Kind.PartitionKey, Column.Type.Int),
                Column.create("pk_2", Column.Kind.PartitionKey, Column.Type.Boolean),
                ck1,
                Column.create("col_1", Column.Kind.Regular, Column.Type.Int),
                Column.create("static", Column.Kind.Static, Column.Type.Int)),
            Collections.emptyList());
    BoundDMLQuery boundDMLQuery =
        createBoundDMLQueryPks(
            table,
            Arrays.asList(
                new PrimaryKey(
                    table,
                    Arrays.asList(
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), "pk_1", Column.Type.Int, 1),
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), "pk_2", Column.Type.Boolean, true),
                        TypedValue.forJavaValue(
                            TypedValue.Codec.testCodec(), ck1.name(), ck1.type(), "v")))));

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    GenericData.Array<GenericData.Record> partitionKeys =
        (GenericData.Array) result.get(SchemaConstants.MUTATION_EVENT_CLUSTERING_KEYS);
    assertThat(partitionKeys.size()).isEqualTo(1);
    GenericData.Record pkRecord1 = partitionKeys.get(0);
    // validate PKs columns
    validateColumn(
        (GenericData.Record) pkRecord1.get(SchemaConstants.CELL_VALUE_COLUMN),
        ck1.type().cqlDefinition(),
        ck1.order().name(),
        ck1.kind().name(),
        ck1.name());
    // validate if byte buffers carry correct data
    validateColumnValue(
        (ByteBuffer) pkRecord1.get(SchemaConstants.CELL_VALUE_VALUE),
        ck1.type().cqlDefinition(),
        "v",
        table);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeNonPkColumns() throws IOException {
    // given
    Column col =
        Column.create(
            "col_1",
            Column.Kind.Regular,
            ImmutableListType.builder().addParameters(Column.Type.Ascii).build());
    Column staticCol = Column.create("static", Column.Kind.Static, Column.Type.Int);

    Table table =
        Table.create(
            "ks_1",
            "table_1",
            Arrays.asList(
                Column.create("pk_1", Column.Kind.PartitionKey, Column.Type.Int),
                Column.create("pk_2", Column.Kind.PartitionKey, Column.Type.Boolean),
                Column.create("ck_1", Column.Kind.Clustering, Column.Type.Ascii),
                col,
                staticCol),
            Collections.emptyList());

    BoundDMLQuery boundDMLQuery =
        createBoundDMLQuery(
            table,
            Arrays.asList(
                constructModification(col, Arrays.asList("a", "b"), Modification.Operation.APPEND),
                constructModification(staticCol, 100, Modification.Operation.REMOVE)));

    // when
    ByteBuffer byteBuffer = QuerySerializer.serializeQuery(boundDMLQuery);

    // then
    GenericRecord result = toGenericRecord(byteBuffer);
    GenericData.Array<GenericData.Record> columns =
        (GenericData.Array) result.get(SchemaConstants.MUTATION_EVENT_CELLS);
    assertThat(columns.size()).isEqualTo(2);
    GenericData.Record colRecord = columns.get(0);
    GenericData.Record staticRecord = columns.get(1);
    // validate PKs columns
    validateColumn(
        (GenericData.Record) colRecord.get(SchemaConstants.CELL_VALUE_COLUMN),
        col.type().cqlDefinition(),
        null,
        col.kind().name(),
        col.name());

    validateColumn(
        (GenericData.Record) staticRecord.get(SchemaConstants.CELL_VALUE_COLUMN),
        staticCol.type().cqlDefinition(),
        null,
        staticCol.kind().name(),
        staticCol.name());
    // validate if byte buffers carry correct data
    validateColumnValue(
        (ByteBuffer) colRecord.get(SchemaConstants.CELL_VALUE_VALUE),
        col.type().cqlDefinition(),
        Arrays.asList("a", "b"),
        table);
    validateColumnValue(
        (ByteBuffer) staticRecord.get(SchemaConstants.CELL_VALUE_VALUE),
        staticCol.type().cqlDefinition(),
        100,
        table);
    // validate cell specific data
    assertThat(colRecord.get(SchemaConstants.CELL_TTL)).isNotNull();
    assertThat(colRecord.get(SchemaConstants.CELL_OPERATION).toString())
        .isEqualTo(Modification.Operation.APPEND.name());

    assertThat(staticRecord.get(SchemaConstants.CELL_TTL)).isNotNull();
    assertThat(staticRecord.get(SchemaConstants.CELL_OPERATION).toString())
        .isEqualTo(Modification.Operation.REMOVE.name());
  }

  private Modification constructModification(
      Column col, Object value, Modification.Operation operation) {
    TypedValue typedValue =
        TypedValue.forJavaValue(TypedValue.Codec.testCodec(), col.name(), col.type(), value);
    return new Modification() {
      @Override
      public ModifiableEntity entity() {
        return ModifiableEntity.of(col);
      }

      @Override
      public Operation operation() {
        return operation;
      }

      @Override
      public TypedValue value() {
        return typedValue;
      }
    };
  }

  private void validateColumnValue(
      ByteBuffer byteBuffer, String cqlDefinition, Object expected, Table table) {
    Column.ColumnType columnType =
        Column.Type.fromCqlDefinitionOf(
            Keyspace.create(
                table.keyspace(),
                Arrays.asList(table),
                Collections.emptyList(),
                Collections.emptyMap(),
                Optional.empty()),
            cqlDefinition);
    TypedValue typedValue =
        TypedValue.forBytesValue(TypedValue.Codec.testCodec(), columnType, byteBuffer);
    assertThat(typedValue.javaValue()).isEqualTo(expected);
  }

  private void validateColumn(
      GenericData.Record column, String cqlDefintion, String order, String kind, String name) {
    assertThat(
            Optional.ofNullable(column.get(SchemaConstants.COLUMN_CQL_DEFINITION))
                .map(Object::toString)
                .orElse(null))
        .isEqualTo(cqlDefintion);
    assertThat(
            Optional.ofNullable(column.get(SchemaConstants.COLUMN_ORDER))
                .map(Object::toString)
                .orElse(null))
        .isEqualTo(order);
    assertThat(
            Optional.ofNullable(column.get(SchemaConstants.COLUMN_KIND))
                .map(Object::toString)
                .orElse(null))
        .isEqualTo(kind);
    assertThat(column.get(SchemaConstants.COLUMN_NAME).toString()).isEqualTo(name);
  }

  private GenericRecord toGenericRecord(ByteBuffer byteBuffer) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(byteBuffer.array());
    DecoderFactory decoderFactory = DecoderFactory.get();
    BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);

    return new GenericDatumReader<GenericRecord>(SchemaConstants.MUTATION_EVENT)
        .read(null, decoder);
  }

  private BoundDMLQuery createBoundDMLQueryPks(Table table, List<PrimaryKey> primaryKeys) {
    return createBoundDMLQuery(
        table,
        OptionalInt.empty(),
        OptionalLong.empty(),
        QueryType.UPDATE,
        primaryKeys,
        Collections.emptyList());
  }

  private BoundDMLQuery createBoundDMLQuery(Table table, List<Modification> columns) {
    return createBoundDMLQuery(
        table,
        OptionalInt.empty(),
        OptionalLong.empty(),
        QueryType.UPDATE,
        Collections.emptyList(),
        columns);
  }

  private BoundDMLQuery createBoundDMLQuery(
      Table table, List<PrimaryKey> primaryKeys, List<Modification> columns) {
    return createBoundDMLQuery(
        table, OptionalInt.empty(), OptionalLong.empty(), QueryType.UPDATE, primaryKeys, columns);
  }

  private BoundDMLQuery createBoundDMLQuery(QueryType queryType) {
    return createBoundDMLQuery(EMPTY_TABLE, OptionalInt.empty(), OptionalLong.empty(), queryType);
  }

  private BoundDMLQuery createBoundDMLQuery(OptionalInt ttl, OptionalLong timestamp) {
    return createBoundDMLQuery(EMPTY_TABLE, ttl, timestamp);
  }

  private BoundDMLQuery createBoundDMLQuery(Table table) {
    return createBoundDMLQuery(table, OptionalInt.empty(), OptionalLong.empty());
  }

  private BoundDMLQuery createBoundDMLQuery(Table table, OptionalInt ttl, OptionalLong timestamp) {
    return createBoundDMLQuery(
        table, ttl, timestamp, QueryType.UPDATE, Collections.emptyList(), Collections.emptyList());
  }

  private BoundDMLQuery createBoundDMLQuery(
      Table table, OptionalInt ttl, OptionalLong timestamp, QueryType queryType) {
    return createBoundDMLQuery(
        table, ttl, timestamp, queryType, Collections.emptyList(), Collections.emptyList());
  }

  private BoundDMLQuery createBoundDMLQuery(
      Table table,
      OptionalInt ttl,
      OptionalLong timestamp,
      QueryType queryType,
      List<PrimaryKey> primaryKeys,
      List<Modification> modifications) {
    return new BoundDMLQuery() {
      @Override
      public QueryType type() {
        return queryType;
      }

      @Override
      public Source<?> source() {
        return null;
      }

      @Override
      public List<TypedValue> values() {
        return Collections.emptyList();
      }

      @Override
      public Table table() {
        return table;
      }

      @Override
      public RowsImpacted rowsUpdated() {
        return new RowsImpacted.Keys(primaryKeys);
      }

      @Override
      public List<Modification> modifications() {
        return modifications;
      }

      @Override
      public OptionalInt ttl() {
        return ttl;
      }

      @Override
      public OptionalLong timestamp() {
        return timestamp;
      }
    };
  }
}
