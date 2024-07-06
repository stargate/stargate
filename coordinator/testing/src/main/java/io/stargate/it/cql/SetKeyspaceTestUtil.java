package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.TestInfo;

public class SetKeyspaceTestUtil {
  static void testSetKeyspace(
      CqlSession session,
      TestInfo testInfo,
      int expectedResults,
      BiConsumer<String, String> testCode) {
    String ksPrefix =
        testInfo.getTestClass().map(c -> c.getSimpleName().toLowerCase() + "_").orElse("");
    // Create 2 keyspaces

    String ks1 = ksPrefix + "_v5_set_ks1";
    String ks2 = ksPrefix + "_v5_set_ks2";
    session.execute(
        "CREATE KEYSPACE "
            + ks1
            + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE KEYSPACE "
            + ks2
            + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");

    // Create 2 tables
    session.execute("CREATE TABLE " + ks1 + ".tab (k int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE " + ks2 + ".tab (k int PRIMARY KEY, v int)");

    testCode.accept(ks1, ks2);

    // check
    assertThat(session.execute("SELECT * FROM " + ks1 + ".tab").all()).hasSize(expectedResults);
    assertThat(session.execute("SELECT * FROM " + ks2 + ".tab").all()).hasSize(expectedResults);
  }
}
