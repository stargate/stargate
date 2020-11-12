package io.stargate.it.http.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthProviderResponse {

  private String accessToken;
  private int expiresIn;
  private int refreshExpiresIn;
  private String refreshToken;
  private String tokenType;
  private int notBeforePolicy;
  private String sessionState;
  private String scope;

  @JsonProperty("access_token")
  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  @JsonProperty("expires_in")
  public int getExpiresIn() {
    return expiresIn;
  }

  public void setExpiresIn(int expiresIn) {
    this.expiresIn = expiresIn;
  }

  @JsonProperty("refresh_expires_in")
  public int getRefreshExpiresIn() {
    return refreshExpiresIn;
  }

  public void setRefreshExpiresIn(int refreshExpiresIn) {
    this.refreshExpiresIn = refreshExpiresIn;
  }

  @JsonProperty("refresh_token")
  public String getRefreshToken() {
    return refreshToken;
  }

  public void setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
  }

  @JsonProperty("token_type")
  public String getTokenType() {
    return tokenType;
  }

  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }

  @JsonProperty("not-before-policy")
  public int getNotBeforePolicy() {
    return notBeforePolicy;
  }

  public void setNotBeforePolicy(int notBeforePolicy) {
    this.notBeforePolicy = notBeforePolicy;
  }

  @JsonProperty("session_state")
  public String getSessionState() {
    return sessionState;
  }

  public void setSessionState(String sessionState) {
    this.sessionState = sessionState;
  }

  @JsonProperty("scope")
  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }
}
