package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Note: all tests related to unset values assume that the protocol version is always 4 or higher.
 */
public class BoundStatementTest extends JavaDriverTestBase {

  private static final String KEY = "test";
  private static final int VALUE = 7;

  public BoundStatementTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setupSchema() {
    // table where every column forms the primary key.
    session.execute("CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))");
    for (int i = 0; i < 100; i++) {
      session.execute(
          SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
              .addPositionalValues(KEY, i)
              .build());
    }

    // table with simple primary key, single cell.
    session.execute("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)");

    // table with composite partition key
    session.execute(
        "CREATE TABLE IF NOT EXISTS test3 "
            + "(pk1 int, pk2 int, v int, "
            + "PRIMARY KEY ((pk1, pk2)))");
  }

  @Test
  public void should_use_positional_values() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, VALUE));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  public void should_use_named_values() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (:k, :v)");
    session.execute(
        prepared.boundStatementBuilder().setString("k", KEY).setInt("v", VALUE).build());

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  public void should_allow_nulls_values() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, null));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.isNull("v0")).isTrue();
  }

  @Test
  @Disabled("Returns an empty result set instead of failing")
  public void should_fail_if_missing_values() {
    PreparedStatement prepared = session.prepare("SELECT v FROM test3 WHERE pk1=? and pk2=?");
    assertThatThrownBy(() -> session.execute(prepared.bind(1)))
        .isInstanceOf(InvalidQueryException.class);
  }

  @Test
  @Disabled("Doesn't seem to work at the moment")
  public void should_not_write_tombstone_if_value_is_implicitly_unset() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, VALUE));

    BoundStatement boundStatement = prepared.bind(KEY);
    assertThat(boundStatement.isSet("v0")).isFalse();
    session.execute(boundStatement);

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  @Disabled("Doesn't seem to work at the moment")
  public void should_not_write_tombstone_if_value_is_explicitly_unset() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    BoundStatement boundStatement = prepared.bind(KEY, VALUE);
    session.execute(boundStatement);

    session.execute(boundStatement.unset("v0"));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }
}
