package io.stargate.auth;

public interface AuthenticationService {
    String createToken(String key, String secret) throws UnauthorizedException;

    StoredCredentials validateToken(String token) throws UnauthorizedException;
}
