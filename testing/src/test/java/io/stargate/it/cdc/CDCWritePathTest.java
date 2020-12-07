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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CDCWritePathTest extends BaseOsgiIntegrationTest {

  private CqlSession session;

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) {
    session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT,
                        Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(180))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(cluster.seedAddress(), 9043))
            .withLocalDatacenter(cluster.datacenter())
            .build();
  }

  @Test
  public void shouldHaveCreatedCDCKeyspace() {
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
  public void shouldHaveCreatedCDCEventsTable() {
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
}
