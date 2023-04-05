package io.stargate.it.http.models;

import java.util.List;
import java.util.Map;

public class KeycloakUser {

  private String username;
  private boolean enabled;
  private boolean emailVerified;
  private Map<String, List<String>> attributes;
  private List<KeycloakCredential> credentials;

  public KeycloakUser() {}

  public KeycloakUser(
      String username,
      boolean enabled,
      boolean emailVerified,
      Map<String, List<String>> attributes,
      List<KeycloakCredential> credentials) {
    this.username = username;
    this.enabled = enabled;
    this.emailVerified = emailVerified;
    this.attributes = attributes;
    this.credentials = credentials;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEmailVerified() {
    return emailVerified;
  }

  public void setEmailVerified(boolean emailVerified) {
    this.emailVerified = emailVerified;
  }

  public Map<String, List<String>> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, List<String>> attributes) {
    this.attributes = attributes;
  }

  public List<KeycloakCredential> getCredentials() {
    return credentials;
  }

  public void setCredentials(List<KeycloakCredential> credentials) {
    this.credentials = credentials;
  }
}
