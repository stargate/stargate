package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE KEYSPACE IF NOT EXISTS vector_type_ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}"
    })
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class VectorTypeTest extends BaseIntegrationTest {
  @BeforeEach
  public void initTable(CqlSession session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS vector_type_ks.vector_type_table (\n"
            + "       pk int PRIMARY KEY,\n"
            + "       val float vector[3],\n"
            + "       str Text)");
  }

  @AfterEach
  public void cleanupTable(CqlSession session) {
    session.execute("DROP TABLE IF EXISTS vector_type_ks.vector_type_table");
  }

  @Test
  @DisplayName("Insert a row (no Vector), read other columns")
  @Order(1)
  public void insertSimpleRowReadWithoutVector(CqlSession session) {
    session.execute(
        SimpleStatement.builder(
                "INSERT into vector_type_ks.vector_type_table (pk, str) values (:pk, :str)")
            .addNamedValue("pk", 123)
            .addNamedValue("str", "text value")
            .build());

    List<Row> rows =
        session
            .execute("select pk,str from vector_type_ks.vector_type_table where pk=123")
            .all();
    assertThat(rows).isNotNull().hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt("pk")).isEqualTo(123);
    assertThat(row.getString("str")).isEqualTo("text value");
  }

  @Test
  @Order(2)
  @DisplayName("Insert a row (no Vector), read all columns including Vector")
  public void insertSimpleRowReadWithVector(CqlSession session) {
    session.execute(
        SimpleStatement.builder(
                "INSERT into vector_type_ks.vector_type_table (pk, str) values (:pk, :str)")
            .addNamedValue("pk", 345)
            .addNamedValue("str", "something")
            .build());

    // But try reading all columns including CT: will currently fail
    List<Row> rows =
        session
            .execute("select ks,str,val from vector_type_ks.vector_type_table where pk=345")
            .all();
    assertThat(rows).isNotNull().hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt("pk")).isEqualTo(345);
    assertThat(row.getString("str")).isEqualTo("something");
    assertThat(row.isNull("val")).isTrue();
  }
}
