package io.stargate.it.cql;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec
public class VectorTypeTest extends BaseIntegrationTest {
  private static final String CREATE_VECTOR_TABLE =
      "CREATE TABLE vector_table ("
          + "id int PRIMARY KEY, "
          + "embedding vector<float, 100> "
          + ")";
  private static final String DROP_VECTOR_TABLE = "DROP TABLE IF EXISTS vector_table";

  @Test
  @DisplayName("Should be able to create a table with Vector column")
  public void createTableInsertAndFetch(CqlSession session) {
    assumeTrue(
        backend.supportsVSearch(),
        "Test disabled if backend does not support Vector Search (vsearch)");

    session.execute(DROP_VECTOR_TABLE);
    session.execute(CREATE_VECTOR_TABLE);
  }
}
