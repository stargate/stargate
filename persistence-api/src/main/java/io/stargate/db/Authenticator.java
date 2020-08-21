package io.stargate.db;

import javax.security.cert.X509Certificate;
import java.net.InetAddress;

import org.apache.cassandra.stargate.exceptions.AuthenticationException;

public interface Authenticator
{
    String getInternalClassName();

    boolean requireAuthentication();

    SaslNegotiator newSaslNegotiator(InetAddress clientAddress, X509Certificate[] certificates);

    interface SaslNegotiator
    {
        byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException;

        boolean isComplete();

        AuthenticatedUser<?> getAuthenticatedUser() throws AuthenticationException;
    }
}
