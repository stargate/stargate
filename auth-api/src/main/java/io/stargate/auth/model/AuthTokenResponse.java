package io.stargate.auth.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * AuthTokenResponse contains an authentication token to be used for future requests.
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuthTokenResponse {
    private String authToken = null;

    public AuthTokenResponse authToken(String authToken) {
        this.authToken = authToken;
        return this;
    }

    @JsonProperty("authToken")
    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthTokenResponse authTokenResponse = (AuthTokenResponse) o;
        return Objects.equals(authToken, authTokenResponse.authToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authToken);
    }

    @Override
    public String toString() {
        return "AuthTokenResponse{" +
                "authToken='" + authToken + '\'' +
                '}';
    }
}
