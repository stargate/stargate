package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
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
                    + "       c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',\n"
                    + "       c2 Text)")
            .all();
    assertThat(rows).isEmpty();
  }
}
