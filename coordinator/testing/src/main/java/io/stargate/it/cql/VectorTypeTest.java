package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlVector;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec
public class VectorTypeTest extends BaseIntegrationTest {
  private static final String CREATE_VECTOR_TABLE =
      "CREATE TABLE IF NOT EXISTS vector_table ("
          + "id int PRIMARY KEY, embedding vector<float, 5> "
          + ")";
  private static final String DROP_VECTOR_TABLE = "DROP TABLE IF EXISTS vector_table";

  @BeforeAll
  public static void validateRunningOnVSearchEnabled(ClusterConnectionInfo backend) {
    assumeTrue(
        backend.supportsVSearch(),
        "Test disabled if backend does not support Vector Search (vsearch)");
  }

  @Test
  @DisplayName("Should be able to create a table with Vector column")
  @Order(1)
  public void createVectorTable(CqlSession session) {
    session.execute(DROP_VECTOR_TABLE);
    session.execute(CREATE_VECTOR_TABLE);
  }

  @Test
  @DisplayName("Should be able to create an index for Vector column")
  @Order(2)
  public void createVectorIndex(CqlSession session) {
    session.execute(CREATE_VECTOR_TABLE);
    session.execute(
        "CREATE CUSTOM INDEX IF NOT EXISTS ann_index "
            + "  ON vector_table(embedding) USING 'StorageAttachedIndex'");
  }

  @Test
  @DisplayName("Should be able to insert and fetch a vector value")
  @Order(3)
  public void insertAndFetch(CqlSession session) {
    session.execute(CREATE_VECTOR_TABLE);
    session.execute(
        "INSERT into vector_table (id, embedding) values (1, [1.0, 0.5, 0.0, 0.125, 0.25])");
    List<Row> rows =
        session.execute("select id, embedding from vector_table where id = ?", 1).all();
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(1);
    CqlVector<Float> v = row.getVector(1, Float.class);
    assertThat(v).hasSize(5);
    assertThat(v).containsExactly(1.0f, 0.5f, 0.0f, 0.125f, 0.25f);
  }
}
