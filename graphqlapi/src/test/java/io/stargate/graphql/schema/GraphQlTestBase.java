package io.stargate.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
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

  @Mock protected Persistence<?, ?, ?> persistence;
  @Mock protected AuthenticationService authenticationService;
  @Mock protected HTTPAwareContextImpl context;
  @Mock protected DataStore dataStore;

  @Mock private StoredCredentials storedCredentials;

  @SuppressWarnings("rawtypes")
  @Mock
  private ClientState clientState;

  @SuppressWarnings("rawtypes")
  @Mock
  private QueryState queryState;

  @Captor protected ArgumentCaptor<String> queryCaptor;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setupEnvironment() {

    // Mock authentication:
    try {
      String token = "mock token";
      String roleName = "mock role name";
      when(context.getAuthToken()).thenReturn(token);
      when(authenticationService.validateToken(token)).thenReturn(storedCredentials);
      when(storedCredentials.getRoleName()).thenReturn(roleName);
      when(persistence.newClientState(roleName)).thenReturn(clientState);
      when(persistence.newQueryState(clientState)).thenReturn(queryState);
      when(persistence.newDataStore(queryState, null)).thenReturn(dataStore);
    } catch (Exception e) {
      fail("Unexpected exception while mocking authentication", e);
    }

    // Make every query return an empty result set.
    // At the time of writing, this is enough to execute all of our fetchers without failure. It
    // might have to evolve in the future if queries start checking particular elements in the
    // response (like the '[applied]' column for LWTs).
    //
    // As a consequence, our unit tests do not cover the conversion of CQL results back to GraphQL.
    // We would have to mock the entire result API (ResultSet, Row, Column, ColumnType...), which is
    // a bit overkill and brittle. The integration tests in the 'testing' module fill that gap.
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.rows()).thenReturn(Collections.emptyList());
    when(dataStore.query(queryCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(resultSet));

    graphQl = GraphQL.newGraphQL(createGraphQlSchema()).build();
  }

  protected abstract GraphQLSchema createGraphQlSchema();

  /**
   * Convenience method to execute a GraphQL query.
   *
   * <p>You can also access {@link #graphQl} directly in subclasses, but don't forget to set {@link
   * #context} on the execution input.
   */
  protected ExecutionResult executeGraphQl(String query) {
    return graphQl.execute(ExecutionInput.newExecutionInput(query).context(context).build());
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it generates the given CQL query.
   */
  protected void assertSuccess(String graphQlQuery, String expectedCqlQuery) {
    ExecutionResult result = executeGraphQl(graphQlQuery);
    assertThat(result.getErrors()).isEmpty();
    assertThat(queryCaptor.getValue()).isEqualTo(expectedCqlQuery);
  }

  /**
   * Convenience method to execute a GraphQL query and assert that it generates an error containing
   * the given message.
   */
  protected void assertError(String graphQlQuery, String expectedError) {
    ExecutionResult result = executeGraphQl(graphQlQuery);
    assertThat(result.getErrors()).as("Expected an error but the query succeeded").isNotEmpty();
    GraphQLError error = result.getErrors().get(0);
    assertThat(error.getMessage()).contains(expectedError);
  }
}
