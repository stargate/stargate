package io.stargate.auth;

public enum Scope {
  READ,
  MODIFY, // includes INSERT and UPDATE of data
  DELETE, // encompasses deleting a row, dropping a table, or deleting a keyspace

  // following are specific to schema resources
  CREATE,
  ALTER, // applies to schema related resources
  TRUNCATE
}
