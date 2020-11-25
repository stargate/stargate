package io.stargate.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
import io.stargate.auth.StoredCredentials;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.web.HttpAwareContext;
import io.stargate.graphql.web.HttpAwareContext.BatchContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public abstract class GraphQlTestBase {

  private final String token = "mock token";
  protected GraphQL graphQl;
  protected GraphQLSchema graphQlSchema;

  @Mock protected Persistence persistence;
  @Mock protected AuthenticationService authenticationService;
  @Mock protected ResultSet resultSet;
  @Mock private StoredCredentials storedCredentials;

  @Captor protected ArgumentCaptor<String> queryCaptor;
  @Captor protected ArgumentCaptor<List<String>> batchCaptor;
  @Captor protected ArgumentCaptor<Parameters> parametersCaptor;

  private MockedStatic<DataStore> dataStoreCreateMock;

  // Stores the parameters of the last batch execution
  protected Parameters batchParameters;

  @BeforeEach
  public void setupEnvironment() {
    try {
      String roleName = "mock role name";
      when(authenticationService.validateToken(token)).thenReturn(storedCredentials);
      when(storedCredentials.getRoleName()).thenReturn(roleName);
      dataStoreCreateMock = mockStatic(DataStore.class);
      dataStoreCreateMock
          .when(() -> DataStore.create(eq(persistence), eq(roleName), parametersCaptor.capture()))
          .then(
              i -> {
                DataStore dataStore = mock(DataStore.class);
                when(dataStore.query(queryCaptor.capture()))
                    .thenReturn(CompletableFuture.completedFuture(resultSet));

                // Batches use multiple data store instances, one per each mutation
                // We need to capture the parameters provided at dataStore creation
                Parameters dataStoreParameters = i.getArgument(2, Parameters.class);
                when(dataStore.batch(batchCaptor.capture()))
                    .then(
                        batchInvoke -> {
                          batchParameters = dataStoreParameters;
                          return CompletableFuture.completedFuture(mock(ResultSet.class));
                        });

                Schema schema = createCqlSchema();
                if (schema != null) {
                  when(dataStore.schema()).thenReturn(schema);
                }
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

  @AfterEach
  public void resetMocks() {
    if (dataStoreCreateMock != null) {
      dataStoreCreateMock.close();
    }
  }

  /**
   * Allows subclasses to mock the schema metadata that will be returned by the persistence layer.
   * This is useful for DDL fetcher tests that create a {@link DdlSchemaBuilder} in {@link
   * #createGraphQlSchema()} (this method is invoked first, so any keyspace created here will be
   * visible when {@link #createGraphQlSchema()} runs).
   */
  protected Schema createCqlSchema() {
    return null;
  }

  protected abstract GraphQLSchema createGraphQlSchema();

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

    when(context.getAuthToken()).thenReturn(token);
    when(context.getBatchContext()).thenReturn(batchContext);
    return graphQl.execute(ExecutionInput.newExecutionInput(query).context(context).build());
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it generates the given CQL query.
   */
  protected void assertQuery(String graphqlQuery, String expectedCqlQuery) {
    ExecutionResult result = executeGraphQl(graphqlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat(queryCaptor.getValue()).isEqualTo(expectedCqlQuery);
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
