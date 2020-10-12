package io.stargate.graphql.schema;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
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

    graphQl = GraphQL.newGraphQL(createGraphQlSchema()).build();
  }

  protected abstract GraphQLSchema createGraphQlSchema();

  /**
   * Convenience method to execute a string query.
   *
   * <p>You can also access {@link #graphQl} directly in subclasses, but don't forget to set {@link
   * #context} on the execution input.
   */
  protected ExecutionResult executeGraphQl(String query) {
    return graphQl.execute(ExecutionInput.newExecutionInput(query).context(context).build());
  }
}
