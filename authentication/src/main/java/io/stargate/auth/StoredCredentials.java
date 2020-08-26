package io.stargate.auth;

import java.util.Objects;

/**
 * StoredCredentials are a roleName and password mapping to one or more key-secret pairs.
 **/
public class StoredCredentials {
    private String roleName = null;
    private String password = null;

    public StoredCredentials roleName(String roleName) {
        this.roleName = roleName;
        return this;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public StoredCredentials password(String password) {
        this.password = password;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoredCredentials storedCredentials = (StoredCredentials) o;
        return Objects.equals(roleName, storedCredentials.roleName) &&
                Objects.equals(password, storedCredentials.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleName, password);
    }

    @Override
    public String toString() {
        return "StoredCredentials{" +
                "roleName='" + roleName + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
