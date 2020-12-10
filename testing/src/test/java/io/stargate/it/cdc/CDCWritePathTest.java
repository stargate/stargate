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
package io.stargate.it.cdc;

import static io.stargate.db.cdc.datastore.CDCQueryBuilder.DEFAULT_CDC_EVENTS_TABLE;
import static io.stargate.db.cdc.datastore.CDCQueryBuilder.DEFAULT_CDC_KEYSPACE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.db.cdc.datastore.CDCQueryBuilder;
import io.stargate.db.cdc.serde.avro.SchemaConstants;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.web.models.ColumnModel;
import io.stargate.web.models.RowAdd;
import io.stargate.web.models.RowsResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.http.HttpStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(createKeyspace = false, dropKeyspace = false)
public class CDCWritePathTest extends BaseOsgiIntegrationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static String host;
  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromCql("cdc_tables");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromCql("cdc_enabled_table");
  private static String authToken;

  @BeforeAll
  public static void setup(CqlSession session, StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            KEYSPACE.asCql(false)));

    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, value text)",
            KEYSPACE.asCql(false), TABLE.asCql(false)));
    initAuth();
  }

  private static void initAuth() throws IOException {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    Assertions.assertThat(authToken).isNotNull();
  }

  @Test
  public void shouldHaveCreatedCDCKeyspace(CqlSession session) {
    // when
    Row row =
        session
            .execute(
                String.format(
                    "select * from system_schema.keyspaces WHERE keyspace_name = '%s'",
                    DEFAULT_CDC_KEYSPACE))
            .all()
            .get(0);
    // then
    assertThat(row).isNotNull();
    Map<String, String> replication = row.getMap("replication", String.class, String.class);
    assertThat(replication.get("replication_factor")).isEqualTo("1");
    assertThat(replication.get("class")).isEqualTo("org.apache.cassandra.locator.SimpleStrategy");
  }

  @Test
  public void shouldHaveCreatedCDCEventsTable(CqlSession session) {
    // when
    TableMetadata tableMetadata =
        session
            .getMetadata()
            .getKeyspace(DEFAULT_CDC_KEYSPACE)
            .flatMap(keyspace -> keyspace.getTable(DEFAULT_CDC_EVENTS_TABLE))
            .get();

    // then
    Map<CqlIdentifier, ColumnMetadata> columns = tableMetadata.getColumns();
    assertThat(columns.get(CqlIdentifier.fromCql("shard")).getType()).isEqualTo(DataTypes.INT);
    assertThat(columns.get(CqlIdentifier.fromCql("event_id")).getType())
        .isEqualTo(DataTypes.TIMEUUID);
    assertThat(columns.get(CqlIdentifier.fromCql("delivered_on_stream")).getType())
        .isEqualTo(DataTypes.setOf(DataTypes.UUID));
    assertThat(columns.get(CqlIdentifier.fromCql("payload")).getType()).isEqualTo(DataTypes.BLOB);
    assertThat(columns.get(CqlIdentifier.fromCql("version")).getType()).isEqualTo(DataTypes.INT);
  }

  @Test
  public void shouldSaveRecordToCDCEventsWhenInsertToCDCEnabledTableUsingRestApi(CqlSession session)
      throws IOException {
    // given
    List<ColumnModel> columns = new ArrayList<>();

    ColumnModel idColumn = new ColumnModel();
    idColumn.setName("pk");
    idColumn.setValue("1");
    columns.add(idColumn);

    ColumnModel firstNameColumn = new ColumnModel();
    firstNameColumn.setName("value");
    firstNameColumn.setValue("Update Value");
    columns.add(firstNameColumn);

    // when
    addRow(columns);

    // then
    List<Row> all =
        session
            .execute(
                String.format(
                    "SELECT * FROM %s.%s", DEFAULT_CDC_KEYSPACE, DEFAULT_CDC_EVENTS_TABLE))
            .all();
    assertThat(all.size()).isEqualTo(1);
    Row row = all.get(0);
    assertThat(row.getInt(CDCQueryBuilder.CDCEventsColumns.SHARD.getName())).isNotNull();
    assertThat(row.getUuid(CDCQueryBuilder.CDCEventsColumns.EVENT_ID.getName())).isNotNull();
    assertThat(
            row.getSet(CDCQueryBuilder.CDCEventsColumns.DELIVERED_ON_STREAMS.getName(), UUID.class)
                .size())
        .isEqualTo(0);
    assertThat(row.getInt(CDCQueryBuilder.CDCEventsColumns.VERSION.getName())).isEqualTo(0);
    ByteBuffer payload = row.getByteBuffer(CDCQueryBuilder.CDCEventsColumns.PAYLOAD.getName());
    assertThat(payload.array()).isNotEmpty();
    assertThat(toGenericRecord(payload)).isNotNull();
  }

  private void addRow(List<ColumnModel> columns) throws IOException {
    RowAdd rowAdd = new RowAdd();
    rowAdd.setColumns(columns);

    String body =
        RestUtils.post(
            authToken,
            String.format("%s:8082/v1/keyspaces/%s/tables/%s/rows", host, KEYSPACE, TABLE),
            objectMapper.writeValueAsString(rowAdd),
            HttpStatus.SC_CREATED);

    RowsResponse rowsResponse = objectMapper.readValue(body, new TypeReference<RowsResponse>() {});
    Assertions.assertThat(rowsResponse.getRowsModified()).isEqualTo(1);
    Assertions.assertThat(rowsResponse.getSuccess()).isTrue();
  }

  private GenericRecord toGenericRecord(ByteBuffer byteBuffer) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(byteBuffer.array());
    DecoderFactory decoderFactory = DecoderFactory.get();
    BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);

    return new GenericDatumReader<GenericRecord>(SchemaConstants.MUTATION_EVENT)
        .read(null, decoder);
  }
}
