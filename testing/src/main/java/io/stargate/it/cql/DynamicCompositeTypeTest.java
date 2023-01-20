package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class DynamicCompositeTypeTest extends BaseIntegrationTest {
  @Test
  @DisplayName("Try to create Table with DynamicCompositeType")
  public void createDynamicCompositeTypeTable(CqlSession session) {
    // Cannot use "CreateTable" since we seem not to have support for building dynamic
    // composite type. Instead, straight CQL:
    List<Row> rows =
        session
            .execute(
                "CREATE TABLE dynamic_composite_table (\n"
                    + "       k int PRIMARY KEY,\n"
                    + "       dct 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',\n"
                    + "       stuff Text)")
            .all();
    assertThat(rows).isEmpty();

    // Ok let's see if we can actually insert rows (without trying to set DCT)
    session.execute(
        SimpleStatement.builder("INSERT into dynamic_composite_table (k, stuff) values (:k, :v)")
            .addNamedValue("k", 123)
            .addNamedValue("v", "text value")
            .build());

    // And then fetch row inserted as well, first without accessing DCT
    rows = session.execute("select k,stuff from dynamic_composite_table where k=123").all();
    assertThat(rows).isNotNull().hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt("k")).isEqualTo(123);
    assertThat(row.getString("stuff")).isEqualTo("text value");

    // and then getting DCT too (missing/null)
    // !!! Fails here at this point
    /*
    row = session.execute("select k,stuff,dct from dynamic_composite_table where k=123").one();
    assertThat(row.getInt("k")).isEqualTo(123);
    assertThat(row.getString("stuff")).isEqualTo("text value");
    assertThat(row.isNull("dct")).isTrue();
     */
  }
}
