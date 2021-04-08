package io.stargate.it.http.models;

public class KeycloakCredential {

  private String type;
  private String value;
  private String temporary;

  public KeycloakCredential() {}

  public KeycloakCredential(String type, String value, String temporary) {
    this.type = type;
    this.value = value;
    this.temporary = temporary;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getTemporary() {
    return temporary;
  }

  public void setTemporary(String temporary) {
    this.temporary = temporary;
  }
}
