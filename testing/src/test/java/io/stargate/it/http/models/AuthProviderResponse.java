package io.stargate.it.http.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthProviderResponse {

  /*
    {
    "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJpeDV1cUI1clJmdGlxV2VrU1NDdlNMV0lRNGpqZThIODhHZ0tNZThrZkhnIn0.eyJleHAiOjE2MDQ5NjQxMTQsImlhdCI6MTYwNDk2MzUxNCwianRpIjoiYjViNDdlZmItZGUwMy00NDI1LTk5MDctNjU5MDNhNzA1ZmM2IiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo0NDQ0L2F1dGgvcmVhbG1zL3N0YXJnYXRlIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6Ijc5NGIyMTY0LTZkYWMtNGRlYS1hODA0LTgzN2MyNWMwZGI4MSIsInR5cCI6IkJlYXJlciIsImF6cCI6InVzZXItc2VydmljZSIsInNlc3Npb25fc3RhdGUiOiJkOWNiMjc5ZS1lYjE1LTQ2ZGMtOWY1Ny1mNjhiOTliMmQyMmIiLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwic3RhcmdhdGVfY2xhaW1zIjp7Ingtc3RhcmdhdGUtcm9sZSI6IndlYl91c2VyIiwieC1zdGFyZ2F0ZS11c2VyaWQiOiI5ODc2In0sInByZWZlcnJlZF91c2VybmFtZSI6InRlc3R1c2VyMSJ9.T9Nv0ka451I1ISNMPpszRbBR8VxFmZqOCSLCb99clhjzm1EipIJZ9sgW-d-klREBmFCaz1apIU7THwD8uSrz7oCMDbAkIBPCHdcHJg5mz3jR5jWur3HigVHyVZLnxMEQsLZ9sC4hhmbrZwlYWuqgd1D0K40vigpB6IYzI5PgE8tJM3TJCx0aQqAumhVXXyzT_X85KKTaz3MxtxUTzyhe-pMB6NU_kH7ZDgzY1rku9qmLVfFX1i6r9jh9GstnkFOM_Ut1k0PFHVn4e5srgQZn9wi4T7llw7LQ-1bEdldSRNSwDvTj8cSf_4LseiZiofDeQZtx18cxB86LliTMiHqY1Q",
    "expires_in": 600,
    "refresh_expires_in": 1800,
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI0OWMwMDBmMi1hY2I4LTQxNDQtOGEwYS03OTRjOGNiOWQ3MGQifQ.eyJleHAiOjE2MDQ5NjUzMTQsImlhdCI6MTYwNDk2MzUxNCwianRpIjoiYzY0ZjNhOTEtN2QxMi00MWNhLThhMzctZjAwNjZiNTc3MjRjIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo0NDQ0L2F1dGgvcmVhbG1zL3N0YXJnYXRlIiwiYXVkIjoiaHR0cDovL2xvY2FsaG9zdDo0NDQ0L2F1dGgvcmVhbG1zL3N0YXJnYXRlIiwic3ViIjoiNzk0YjIxNjQtNmRhYy00ZGVhLWE4MDQtODM3YzI1YzBkYjgxIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6InVzZXItc2VydmljZSIsInNlc3Npb25fc3RhdGUiOiJkOWNiMjc5ZS1lYjE1LTQ2ZGMtOWY1Ny1mNjhiOTliMmQyMmIiLCJzY29wZSI6InByb2ZpbGUgZW1haWwifQ.d5x6FM-MMHkZU57_nwul5y-hVvhIlPgsrOjLTF-Gl80",
    "token_type": "bearer",
    "not-before-policy": 0,
    "session_state": "d9cb279e-eb15-46dc-9f57-f68b99b2d22b",
    "scope": "profile email"
  }
     */
  String accessToken;
  int expiresIn;
  int refreshExpiresIn;
  String refreshToken;
  String tokenType;
  int notBeforePolicy;
  String sessionState;
  String scope;

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
