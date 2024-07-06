package io.stargate.it.cql;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.driver.WithProtocolVersion;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
public abstract class AbstractTypeTest extends BaseIntegrationTest {
  private static List<TypeSample<?>> allTypes;

  @BeforeEach
  public void createSchema(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {

    allTypes = generateAllTypes(keyspaceId);

    // Creating a new table for each type is too slow, use a single table with all possible types:
    CreateTable createTableQuery =
        createTable("test").ifNotExists().withPartitionKey("k", DataTypes.INT);
    for (TypeSample<?> sample : allTypes) {
      createTableQuery = createTableQuery.withColumn(sample.columnName, sample.cqlType);
    }

    session.execute(createTableQuery.asCql());
  }

  @SuppressWarnings("unused") // called by JUnit 5
  public static List<TypeSample<?>> getAllTypes() {
    return allTypes;
  }

  @DisplayName("Should write and read data type")
  @ParameterizedTest
  @MethodSource("getAllTypes")
  public <JavaTypeT> void writeAndReadTest(TypeSample<JavaTypeT> sample, CqlSession session) {

    String insertQuery =
        insertInto("test").value("k", literal(1)).value(sample.columnName, bindMarker()).asCql();

    SimpleStatement simpleStatement = SimpleStatement.newInstance(insertQuery, sample.value);
    session.execute(simpleStatement);
    checkValue(sample, session);

    session.execute(BatchStatement.newInstance(BatchType.LOGGED).add(simpleStatement));
    checkValue(sample, session);

    session.execute(session.prepare(insertQuery).bind(sample.value));
    checkValue(sample, session);
  }

  private <JavaTypeT> void checkValue(TypeSample<JavaTypeT> sample, CqlSession session) {
    String selectQuery =
        selectFrom("test").column(sample.columnName).whereColumn("k").isEqualTo(literal(1)).asCql();
    Row row = session.execute(selectQuery).one();
    assertThat(row).isNotNull();
    assertThat(row.get(0, sample.javaType)).isEqualTo(sample.value);

    // Clean up for the following tests
    session.execute("DELETE FROM test WHERE k = 1");
  }

  protected abstract List<TypeSample<?>> generateAllTypes(CqlIdentifier keyspaceId);

  @WithProtocolVersion("V5")
  public static class WithV5ProtocolVersionTest extends DurationTypeTest {}
}
