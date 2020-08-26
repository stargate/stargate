package io.stargate.auth.file;

import org.junit.jupiter.api.Test;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class AuthServiceTest {

    @Test
    void createToken() throws UnauthorizedException {
        AuthenticationService authService = new AuthFileService();
        String token = authService.createToken("fizz", "buzz");

        assertNotEquals(null, token, "Token should not be null");

        StoredCredentials storedCredentials = authService.validateToken(token);
        assertNotEquals(null, storedCredentials, "StoredCredentials should not be null");
        assertEquals("fizz", storedCredentials.getRoleName());
        assertEquals("buzz", storedCredentials.getPassword());
        System.out.println(storedCredentials.toString());
    }
}