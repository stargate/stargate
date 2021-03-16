package io.stargate.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.web.HttpAwareContext;
import io.stargate.graphql.web.HttpAwareContext.BatchContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public abstract class GraphQlTestBase {
  protected GraphQL graphQl;
  protected GraphQLSchema graphQlSchema;
  @Mock protected AuthenticationService authenticationService;
  @Mock protected AuthorizationService authorizationService;
  @Mock protected ResultSet resultSet;
  @Mock private AuthenticationSubject authenticationSubject;
  @Mock protected DataStoreFactory dataStoreFactory;

  @Captor private ArgumentCaptor<BoundQuery> queryCaptor;
  @Captor protected ArgumentCaptor<Callable<ResultSet>> actionCaptor;
  @Captor private ArgumentCaptor<List<BoundQuery>> batchCaptor;
  @Captor protected ArgumentCaptor<DataStoreOptions> dataStoreOptionsCaptor;

  // Stores the parameters of the last batch execution
  protected Parameters batchParameters;

  @BeforeEach
  public void setupEnvironment() {
    try {
      Schema schema = getCQLSchema();
      String roleName = "mock role name";
      when(authenticationSubject.roleName()).thenReturn(roleName);
      when(authenticationSubject.asUser()).thenCallRealMethod();
      when(authorizationService.authorizedDataRead(
              actionCaptor.capture(),
              eq(authenticationSubject),
              anyString(),
              anyString(),
              any(),
              eq(SourceAPI.GRAPHQL)))
          .then(
              i -> {
                return actionCaptor.getValue().call();
              });
      when(dataStoreFactory.create(
              argThat(u -> u.name().equals(roleName)), dataStoreOptionsCaptor.capture()))
          .then(
              i -> {
                DataStore dataStore = mock(DataStore.class);
                when(dataStore.queryBuilder())
                    .thenReturn(new QueryBuilder(schema, Codec.testCodec(), dataStore));
                when(dataStore.execute(queryCaptor.capture()))
                    .thenReturn(CompletableFuture.completedFuture(resultSet));

                // Batches use multiple data store instances, one per each mutation
                // We need to capture the parameters provided at dataStore creation
                DataStoreOptions dataStoreOptions = i.getArgument(1, DataStoreOptions.class);
                when(dataStore.batch(batchCaptor.capture()))
                    .then(
                        batchInvoke -> {
                          batchParameters = dataStoreOptions.defaultParameters();
                          return CompletableFuture.completedFuture(mock(ResultSet.class));
                        });

                when(dataStore.schema()).thenReturn(schema);
                return dataStore;
              });
    } catch (Exception e) {
      fail("Unexpected exception while mocking dataStore", e);
    }

    graphQlSchema = createGraphQlSchema();
    graphQl =
        GraphQL.newGraphQL(graphQlSchema)
            // Use parallel execution strategy for mutations (serial is default)
            .mutationExecutionStrategy(new AsyncExecutionStrategy())
            .build();
  }

  protected abstract GraphQLSchema createGraphQlSchema();

  public abstract Schema getCQLSchema();

  /**
   * Convenience method to execute a GraphQL query.
   *
   * <p>You can also access {@link #graphQl} directly in subclasses.
   */
  protected ExecutionResult executeGraphQl(String query) {
    // Use a context mock per execution
    HttpAwareContext context = mock(HttpAwareContext.class);

    // Use a dedicated batch executor per execution
    BatchContext batchContext = new BatchContext();

    when(context.getSubject()).thenReturn(authenticationSubject);
    when(context.getBatchContext()).thenReturn(batchContext);
    return graphQl.execute(ExecutionInput.newExecutionInput(query).context(context).build());
  }

  private String queryString(BoundQuery boundQuery) {
    // Technically, bound#queryString() has all its value as markers (the values are in
    // bound#values). However, those tests were written with expectedCqlQuery being the query with
    // all the value "inlined". To avoid changing all the tests, we "cheat" a bit by reaching into
    // the underlying BuiltQuery.
    return ((AbstractBound<?>) boundQuery).source().query().toString();
  }

  public String getCapturedQueryString() {
    return queryString(queryCaptor.getValue());
  }

  public List<String> getCapturedBatchQueriesString() {
    return batchCaptor.getValue().stream().map(this::queryString).collect(Collectors.toList());
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it generates the given CQL query.
   */
  protected void assertQuery(String graphqlQuery, String expectedCqlQuery) {
    ExecutionResult result = executeGraphQl(graphqlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat(getCapturedQueryString()).isEqualTo(expectedCqlQuery);
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it returns the given JSON
   * response.
   */
  protected void assertResponse(String graphqlQuery, String expectedJsonResponse) {
    ExecutionResult result = executeGraphQl(graphqlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat((Object) result.getData()).isEqualTo(parseJson(expectedJsonResponse));
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it generates an error containing
   * the given message.
   */
  protected void assertError(String graphqlQuery, String expectedError) {
    ExecutionResult result = executeGraphQl(graphqlQuery);
    assertThat(result.getErrors()).as("Expected an error but the query succeeded").isNotEmpty();
    GraphQLError error = result.getErrors().get(0);
    assertThat(error.getMessage()).contains(expectedError);
  }

  private Map<?, ?> parseJson(String json) {
    try {
      return new ObjectMapper().readValue(json, Map.class);
    } catch (IOException e) {
      Assertions.fail("Unexpected error while parsing " + json, e);
      return null; // never reached
    }
  }
}
