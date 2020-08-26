package io.stargate.auth.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Secret contains the key and secret for authentication
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
public class Secret {
    private String key = null;
    private String secret = null;

    public Secret key(String key) {
        this.key = key;
        return this;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Secret secret(String secret) {
        this.secret = secret;
        return this;
    }

    @JsonProperty("secret")
    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Secret secret = (Secret) o;
        return Objects.equals(this.key, secret.key) &&
                Objects.equals(this.secret, secret.secret);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, secret);
    }

    @Override
    public String toString() {
        return "Secret{" +
                "key='" + key + '\'' +
                ", secret='" + secret + '\'' +
                '}';
    }
}
