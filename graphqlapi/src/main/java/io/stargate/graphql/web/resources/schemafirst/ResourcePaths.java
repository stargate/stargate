package io.stargate.graphql.web.resources.schemafirst;

public class ResourcePaths {

  /**
   * The path under which all schema-first resource reside. Must not clash with {@code /graphql},
   * which is used by the CQL-first API.
   */
  public static final String ROOT = "/graphqlv2";

  public static final String ADMIN = ROOT + "/admin";
  public static final String NAMESPACES = ROOT + "/namespace";
}
