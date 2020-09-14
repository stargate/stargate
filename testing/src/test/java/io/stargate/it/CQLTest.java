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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import net.jcip.annotations.NotThreadSafe;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@NotThreadSafe
public class CQLTest extends BaseOsgiIntegrationTest
{
    private static final Logger logger = LoggerFactory.getLogger(PersistenceTest.class);

    @Rule
    public TestName name = new TestName();

    private String table;
    private String keyspace;
    private CqlSession session;

    @Before
    public void setup()
    {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, false)
                        .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class.getName())
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
                        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                        .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5))
                        .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
                        .build();

        session = CqlSession.builder()
                .withConfigLoader(loader)
                .addContactPoint(new InetSocketAddress(stargateHost, 9043)).build();

        String testName = name.getMethodName();
        testName = testName.substring(0, testName.indexOf("["));
        keyspace = "ks_" + testName;
        table = testName;

    }

    private void createKeyspace()
    {
        session.execute(String.format("CREATE KEYSPACE \"%s\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", keyspace));
    }

    private void createTable()
    {
        createKeyspace();
        session.execute(String.format("CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value text)", keyspace, table));
    }

    private String insertIntoQuery()
    {
        return String.format("INSERT INTO \"%s\".\"%s\" (key, value) values (?, ?)", keyspace, table);
    }

    private String insertIntoQueryNoKeyspace()
    {
        return String.format("INSERT INTO \"%s\" (key, value) values (?, ?)", table);
    }

    private String selectFromQuery(boolean withKey)
    {
        return String.format("SELECT * FROM \"%s\".\"%s\"%s", keyspace, table, withKey ? " WHERE key = ?" : "");
    }

    private String selectFromQueryNoKeyspace()
    {
        return String.format("SELECT * FROM \"%s\"", table);
    }

    @Test
    public void querySystemLocal() throws UnknownHostException
    {
        ResultSet rs = session.execute("SELECT * FROM system.local");
        Iterator<Row> rows = rs.iterator();
        assertThat(rows).hasNext();
        assertThat(rows.next().getInetAddress("listen_address")).isEqualTo(InetAddress.getByName(stargateHost));
    }

    @Test
    public void querySystemPeers()
    {
        ResultSet rs = session.execute("SELECT * FROM system.peers");
        assertThat(rs.all()).isEmpty();
    }

    @Test
    public void querySimple()
    {
        createTable();

        session.execute(SimpleStatement.builder(insertIntoQuery())
                .addPositionalValues("abc", "def")
                .build());

        ResultSet rs = session.execute(SimpleStatement.builder(selectFromQuery(true))
                .addPositionalValue("abc")
                .build());

        Iterator<Row> rows = rs.iterator();
        assertThat(rows).hasNext();

        Row row = rows.next();
        assertThat(row.getString("key")).isEqualTo("abc");
        assertThat(row.getString("value")).isEqualTo("def");
    }

    @Test
    public void preparedSimple()
    {
        createTable();

        PreparedStatement insertPrepared = session.prepare(insertIntoQuery());
        session.execute(insertPrepared.bind("abc", "def"));

        PreparedStatement selectPrepared = session.prepare(selectFromQuery(true));
        ResultSet rs = session.execute(selectPrepared.bind("abc"));

        Iterator<Row> rows = rs.iterator();
        assertThat(rows).hasNext();

        Row row = rows.next();
        assertThat(row.getString("key")).isEqualTo("abc");
        assertThat(row.getString("value")).isEqualTo("def");
    }

    @Test
    public void batchMixed()
    {
        createTable();

        PreparedStatement insertPrepared = session.prepare(insertIntoQuery());

        BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatement(insertPrepared.bind("abc", "def"))
                .addStatement(SimpleStatement.builder(insertIntoQuery())
                        .addPositionalValues("def", "ghi")
                        .build())
                .build();

        session.execute(batch);

        ResultSet rs = session.execute(String.format(selectFromQuery(false)));

        List<Row> rows = rs.all();
        assertThat(rows).hasSize(2);

        assertThat(rows.get(0).getString("key")).isEqualTo("def");
        assertThat(rows.get(0).getString("value")).isEqualTo("ghi");

        assertThat(rows.get(1).getString("key")).isEqualTo("abc");
        assertThat(rows.get(1).getString("value")).isEqualTo("def");
    }

    @Test
    public void schemaMetadataEvents() throws InterruptedException
    {
        assertThat(session.getMetadata().getKeyspace(keyspace)).isNotPresent();

        createTable();

        KeyspaceMetadata ksMetadata = waitFor(() -> session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace)));
        TableMetadata tableMetadata = waitFor(() -> ksMetadata.getTable(CqlIdentifier.fromInternal(table)));

        assertThat(tableMetadata.getColumns()).hasSize(2);
    }

    @Test
    public void serverSideWarnings()
    {
        createKeyspace();

        // Create a table with an integer value for the `sum()` aggregate
        session.execute(String.format("CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value int)", keyspace, table));

        session.execute(SimpleStatement.builder(insertIntoQuery())
                .setTracing(true)
                .addPositionalValues("abc", 42)
                .build());

        ResultSet rs = session.execute(String.format("SELECT sum(value) FROM \"%s\".\"%s\"", keyspace, table));
        List<String> warnings = rs.getExecutionInfo().getWarnings();
        assertThat(warnings).hasSize(1);
        assertThat(warnings).contains("Aggregation query used without partition key");
    }

    @Test
    public void traceQuery() throws UnknownHostException
    {
        createTable();

        ResultSet rs = session.execute(SimpleStatement.builder(insertIntoQuery())
                .setTracing(true)
                .addPositionalValues("abc", "def")
                .build());

        assertThat(rs.getExecutionInfo().getTracingId()).isNotNull();

        QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
        assertThat(trace.getCoordinator()).isEqualTo(InetAddress.getByName(stargateHost));
        assertThat(trace.getRequestType()).isEqualTo("Execute CQL3 query");
        assertThat(trace.getEvents()).isNotEmpty();
    }

    @Test
    public void tracePrepare() throws UnknownHostException
    {
        createTable();

        PreparedStatement insertPrepared = session.prepare(insertIntoQuery());
        ResultSet rs = session.execute(insertPrepared
                .bind("abc", "def")
                .setTracing(true));

        assertThat(rs.getExecutionInfo().getTracingId()).isNotNull();

        QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
        assertThat(trace.getCoordinator()).isEqualTo(InetAddress.getByName(stargateHost));
        assertThat(trace.getRequestType()).isEqualTo("Execute CQL3 prepared query");
        assertThat(trace.getEvents()).isNotEmpty();
    }

    @Test
    public void traceBatch() throws UnknownHostException
    {
        createTable();

        PreparedStatement insertPrepared = session.prepare(insertIntoQuery());

        BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatement(insertPrepared.bind("abc", "def"))
                .addStatement(SimpleStatement.builder(insertIntoQuery())
                        .addPositionalValues("def", "ghi")
                        .build())
                .setTracing(true)
                .build();

        ResultSet rs = session.execute(batch);
        QueryTrace trace = rs.getExecutionInfo().getQueryTrace();
        assertThat(trace.getCoordinator()).isEqualTo(InetAddress.getByName(stargateHost));
        assertThat(trace.getRequestType()).isEqualTo("Execute batch of CQL3 queries");
        assertThat(trace.getEvents()).isNotEmpty();
    }

    @Test
    public void useKeyspace()
    {
        createTable();

        try
        {
            session.execute(SimpleStatement.builder(insertIntoQueryNoKeyspace())
                    .addPositionalValues("abc", "def")
                    .build());

            Assert.fail("Should have thrown InvalidQueryException");
        }
        catch (InvalidQueryException ex)
        {
            assertThat(ex).
                    hasMessage("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        }

        // Switch to keyspace and retry query

        session.execute(String.format("USE \"%s\"", keyspace));

        session.execute(SimpleStatement.builder(insertIntoQueryNoKeyspace())
                .addPositionalValues("abc", "def")
                .build());

        ResultSet rs = session.execute(selectFromQueryNoKeyspace());

        Iterator<Row> rows = rs.iterator();
        assertThat(rows).hasNext();

        Row row = rows.next();
        assertThat(row.getString("key")).isEqualTo("abc");
        assertThat(row.getString("value")).isEqualTo("def");
    }

    @Test
    public void tupleTest()
    {
        createKeyspace();
        session.execute(String.format("CREATE TABLE \"%s\".\"%s\" (key text PRIMARY KEY, value tuple<text,int,int> )", keyspace, table));

        TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT, DataTypes.INT);
        TupleValue tupleValue = tupleType.newValue("hello", 1, 2);
        session.execute(SimpleStatement.builder(insertIntoQuery())
                .setTracing(true)
                .addPositionalValues("abc", tupleValue)
                .build());

        ResultSet rs = session.execute(SimpleStatement.builder(selectFromQuery(true))
                .addPositionalValue("abc")
                .build());

        Iterator<Row> rows = rs.iterator();
        assertThat(rows).hasNext();

        Row row = rows.next();
        assertThat(row.getString("key")).isEqualTo("abc");

        TupleValue tupleReturnValue = row.getTupleValue("value");
        assertThat(tupleReturnValue).isNotNull();
        assertThat(tupleReturnValue.getString(0)).isEqualTo("hello");
        assertThat(tupleReturnValue.getInt(1)).isEqualTo(1);
        assertThat(tupleReturnValue.getInt(2)).isEqualTo(2);
    }

    private static <T> T waitFor(Supplier<Optional<T>> supplier) throws InterruptedException
    {
        for (int i = 0; i < 100; ++i)
        {
            Optional<T> v = supplier.get();
            if (v.isPresent()) {
                return v.get();
            }
            Thread.sleep(100);
        }

        assertThat(false).isTrue();
        return null;
    }

}

