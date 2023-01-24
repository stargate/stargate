package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import java.util.List;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE KEYSPACE IF NOT EXISTS dynamic_comp_ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}"
    })
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DynamicCompositeTypeTest extends BaseIntegrationTest {
  @BeforeEach
  public void initTable(CqlSession session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS dynamic_comp_ks.dynamic_composite_table (\n"
            + "       k int PRIMARY KEY,\n"
            + "       dct 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',\n"
            + "       stuff Text)");
  }

  @AfterEach
  public void cleanupTable(CqlSession session) {
    session.execute("DROP TABLE IF EXISTS dynamic_comp_ks.dynamic_composite_table");
  }

  @Test
  @DisplayName("Insert a row (no DCT), read other columns")
  @Order(1)
  public void insertSimpleRowReadWithoutDCT(CqlSession session) {
    // Ok let's see if we can actually insert rows (without trying to set DCT)
    session.execute(
        SimpleStatement.builder(
                "INSERT into dynamic_comp_ks.dynamic_composite_table (k, stuff) values (:k, :v)")
            .addNamedValue("k", 123)
            .addNamedValue("v", "text value")
            .build());

    // And then fetch row inserted as well, first without accessing DCT
    List<Row> rows =
        session
            .execute("select k,stuff from dynamic_comp_ks.dynamic_composite_table where k=123")
            .all();
    assertThat(rows).isNotNull().hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt("k")).isEqualTo(123);
    assertThat(row.getString("stuff")).isEqualTo("text value");
  }

  @Test
  @Order(2)
  @Ignore("Currently failing due to Stargate CQL codec not handling this correctly")
  @DisplayName("Insert a row (no DCT), read all columns including DCT")
  public void insertSimpleRowReadWithDCT(CqlSession session) {
    // As with first test, insert without DCT value
    session.execute(
        SimpleStatement.builder(
                "INSERT into dynamic_comp_ks.dynamic_composite_table (k, stuff) values (:k, :v)")
            .addNamedValue("k", 345)
            .addNamedValue("v", "something")
            .build());

    // But try reading all columns including DCT: will currently fail
    List<Row> rows =
        session
            .execute("select k,stuff,dct from dynamic_comp_ks.dynamic_composite_table where k=345")
            .all();
    assertThat(rows).isNotNull().hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt("k")).isEqualTo(345);
    assertThat(row.getString("stuff")).isEqualTo("something");
    assertThat(row.isNull("dct")).isTrue();
  }
}
