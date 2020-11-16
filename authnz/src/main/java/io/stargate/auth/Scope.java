package io.stargate.auth;

public enum Scope {
  READ,
  MODIFY, // includes INSERT and UPDATE of data
  DELETE,

  // following are specific to schema resources
  CREATE,
  ALTER,
  DROP,
  TRUNCATE
}
