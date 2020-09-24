package io.stargate.auth.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UsernameCredentials {

  String username;

  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  @JsonCreator
  public UsernameCredentials(@JsonProperty("username") String username) {
    this.username = username;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UsernameCredentials that = (UsernameCredentials) o;

    return Objects.equals(username, that.username);
  }

  @Override
  public int hashCode() {
    return username != null ? username.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "UsernameCredentials{" + "username='" + username + '\'' + '}';
  }
}
