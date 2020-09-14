package org.apache.cassandra.stargate.transport.internal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import io.stargate.db.Persistence;

class PlainTextTokenSaslNegotiator implements Authenticator.SaslNegotiator
{
    private static final Logger logger = LoggerFactory.getLogger(PlainTextTokenSaslNegotiator.class);

    static final String TOKEN_USERNAME = System.getProperty("stargate.cql_token_username", "__token__");
    static final int TOKEN_MAX_LENGTH = Integer.parseInt(System.getProperty("stargate.cql_token_max_length", "36"));

    static final byte NUL = 0;

    private final Persistence persistence;
    private final AuthenticationService authentication;
    private final Authenticator.SaslNegotiator wrapped;
    StoredCredentials credentials;
    private String username;
    private String password;

    PlainTextTokenSaslNegotiator(Authenticator.SaslNegotiator wrapped,
                                 Persistence persistence,
                                 AuthenticationService authentication)
    {
        this.persistence = persistence;
        this.authentication = authentication;
        this.wrapped = wrapped;
    }

    @Override
    public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
    {
        decodeCredentials(clientResponse);
        if (username.equals(TOKEN_USERNAME))
        {
            try
            {
                logger.trace("Attempting to validate token");
                if (password.length() > TOKEN_MAX_LENGTH)
                {
                    logger.error("Token was too long ({} characters)", password.length());
                    invalidCredentials();
                }

                credentials = authentication.validateToken(password);
                if (credentials == null)
                {
                    logger.error("Null credentials returned from authentication service");
                    invalidCredentials();
                }
            }
            catch (UnauthorizedException e)
            {
                logger.error("Unable to validate token", e);
                invalidCredentials();
            }
        }
        else
        {
            wrapped.evaluateResponse(clientResponse);
        }
        return null;
    }

    @Override
    public boolean isComplete()
    {
        return credentials != null || wrapped.isComplete();
    }

    @Override
    public AuthenticatedUser<?> getAuthenticatedUser() throws AuthenticationException
    {
        if (credentials != null)
            return persistence.newAuthenticatedUser(credentials.getRoleName());
        else
            return wrapped.getAuthenticatedUser();
    }

    /**
     * Copy of the private method:
     * org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator#decodeCredentials(byte[]).
     *
     * @param bytes encoded credentials string sent by the client
     * @throws org.apache.cassandra.exceptions.AuthenticationException if either the
     *         authnId or password is null
     */
    private void decodeCredentials(byte[] bytes) throws org.apache.cassandra.exceptions.AuthenticationException
    {
        logger.trace("Decoding credentials from client token");
        byte[] user = null;
        byte[] pass = null;
        int end = bytes.length;
        for (int i = bytes.length - 1; i >= 0; i--)
        {
            if (bytes[i] == NUL)
            {
                if (pass == null)
                    pass = Arrays.copyOfRange(bytes, i + 1, end);
                else if (user == null)
                    user = Arrays.copyOfRange(bytes, i + 1, end);
                else
                    throw new org.apache.cassandra.exceptions.AuthenticationException("Credential format error: username or password is empty or contains NUL(\\0) character");

                end = i;
            }
        }

        if (pass == null || pass.length == 0)
            throw new org.apache.cassandra.exceptions.AuthenticationException("Password must not be null");
        if (user == null || user.length == 0)
            throw new org.apache.cassandra.exceptions.AuthenticationException("Authentication ID must not be null");

        username = new String(user, StandardCharsets.UTF_8);
        password = new String(pass, StandardCharsets.UTF_8);
    }

    private void invalidCredentials()
    {
        throw new AuthenticationException(String.format("Provided username %s and/or password are incorrect", username));
    }
}
