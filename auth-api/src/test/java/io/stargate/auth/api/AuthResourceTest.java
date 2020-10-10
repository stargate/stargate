package io.stargate.auth.api;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Secret;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(DropwizardExtensionsSupport.class)
class AuthResourceTest {

    private static final AuthenticationService authService = mock(AuthenticationService.class);

    private static final ResourceExtension resource = ResourceExtension.builder()
            .addResource(new AuthResource(authService))
            .build();

    @AfterEach
    void tearDown() {
        reset(authService);
    }

    @Test
    void createTokenFromSecret_success() throws UnauthorizedException {
        Secret secret = new Secret("key", "secret");
        when(authService.createToken("key", "secret")).thenReturn("token");

        AuthTokenResponse token = resource.target("/v1/auth/token/generate")
                .request()
                .post(Entity.entity(secret, MediaType.APPLICATION_JSON), AuthTokenResponse.class);

        assertThat(token.getAuthToken()).isEqualTo("token");
    }

    @Test
    void createTokenFromSecret_unauthorized() throws UnauthorizedException {
        Secret secret = new Secret("key", "secret");
        when(authService.createToken("key", "secret")).thenThrow(UnauthorizedException.class);

        Response response = resource.target("/v1/auth/token/generate")
                .request()
                .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

        assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
    }

    @Test
    void createTokenFromSecret_internalServerError() throws UnauthorizedException {
        Secret secret = new Secret("key", "secret");
        when(authService.createToken("key", "secret")).thenThrow(RuntimeException.class);

        Response response = resource.target("/v1/auth/token/generate")
                .request()
                .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

        assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(500);
    }

    @Test
    void createTokenFromSecret_nullSecret() {
        Response response = resource.target("/v1/auth/token/generate")
                .request()
                .post(null);

        assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    }

    @Test
    void createTokenFromSecret_emptyKey() {
        Secret secret = new Secret("", "secret");

        Response response = resource.target("/v1/auth/token/generate")
                .request()
                .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

        assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    }

    @Test
    void createTokenFromSecret_emptySecret() {
        Secret secret = new Secret("key", "");

        Response response = resource.target("/v1/auth/token/generate")
                .request()
                .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

        assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    }
}
