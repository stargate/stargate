package io.stargate.auth;

public class AuthenticationPrincipal {

  private String token;
  private String roleName;
  private boolean fromExternalAuth;

  public AuthenticationPrincipal(String token, String roleName, boolean fromExternalAuth) {
    this.token = token;
    this.roleName = roleName;
    this.fromExternalAuth = fromExternalAuth;
  }

  public AuthenticationPrincipal(String token, String roleName) {
    this.token = token;
    this.roleName = roleName;
    this.fromExternalAuth = false;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public boolean isFromExternalAuth() {
    return fromExternalAuth;
  }

  public void setFromExternalAuth(boolean fromExternalAuth) {
    this.fromExternalAuth = fromExternalAuth;
  }
}
