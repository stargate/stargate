package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(customOptions = "customizePageSize")
public class PaginationTest extends BaseIntegrationTest {

  private static final String QUERY = "SELECT v FROM test WHERE k = ?";
  private static final String KEY = "test";
  private static final int STATEMENT_PAGE_SIZE = 10;
  private static final int SESSION_PAGE_SIZE = STATEMENT_PAGE_SIZE * 2;
  private static final int TOTAL_COUNT = SESSION_PAGE_SIZE * 5 - 1;

  public static void customizePageSize(OptionsMap config) {
    config.put(TypedDriverOption.REQUEST_PAGE_SIZE, SESSION_PAGE_SIZE);
  }

  @BeforeAll
  public static void setupSchema(CqlSession session) {
    session.execute("CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))");
    for (int i = 0; i < TOTAL_COUNT; i++) {
      session.execute("INSERT INTO test (k, v) VALUES (?, ?)", KEY, i);
    }
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("statementProviders")
  @DisplayName("Should paginate results")
  public void paginationTest(
      @SuppressWarnings("unused") String description,
      StatementProvider statementProvider,
      int expectedPageSize,
      CqlSession session) {
    assertResultsPaginated(statementProvider.apply(session), expectedPageSize, session);
  }

  public static Stream<Arguments> statementProviders() {
    return Stream.of(
        arguments(
            "Simple statement using the session's page size",
            (StatementProvider) session -> SimpleStatement.newInstance(QUERY, KEY),
            SESSION_PAGE_SIZE),
        arguments(
            "Simple statement using a custom page size",
            (StatementProvider)
                session -> SimpleStatement.newInstance(QUERY, KEY).setPageSize(STATEMENT_PAGE_SIZE),
            STATEMENT_PAGE_SIZE),
        arguments(
            "Bound statement using the session's page size",
            (StatementProvider) session -> session.prepare(QUERY).bind(KEY),
            SESSION_PAGE_SIZE),
        arguments(
            "Bound statement using a custom page size",
            (StatementProvider)
                session -> session.prepare(QUERY).bind(KEY).setPageSize(STATEMENT_PAGE_SIZE),
            STATEMENT_PAGE_SIZE));
  }

  private interface StatementProvider extends Function<CqlSession, Statement<?>> {}

  private void assertResultsPaginated(
      Statement<?> statement, int expectedPageSize, CqlSession session) {
    session
        .executeAsync(statement)
        .thenCompose(rs -> assertResultsPaginated(rs, 0, expectedPageSize))
        .toCompletableFuture()
        .join();
  }

  private CompletionStage<Void> assertResultsPaginated(
      AsyncResultSet resultSet, int expectedPage, int expectedPageSize) {
    int currentValue = expectedPage * expectedPageSize;
    for (Row row : resultSet.currentPage()) {
      assertThat(row.getInt("v")).isEqualTo(currentValue);
      currentValue += 1;
    }
    assertThat(currentValue)
        .isEqualTo(Math.min((expectedPage + 1) * expectedPageSize, TOTAL_COUNT));
    if (currentValue == TOTAL_COUNT) {
      assertThat(resultSet.hasMorePages()).isFalse();
      return CompletableFuture.completedFuture(null);
    } else {
      assertThat(resultSet.hasMorePages()).isTrue();
      return resultSet
          .fetchNextPage()
          .thenCompose(rs -> assertResultsPaginated(rs, expectedPage + 1, expectedPageSize));
    }
  }
}
