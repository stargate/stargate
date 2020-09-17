package io.stargate.graphql.fetchers;

import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;

public class SchemaDataFetcherFactory {
  private final Persistence persistence;
  private AuthenticationService authenticationService;

  public SchemaDataFetcherFactory(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  public io.stargate.graphql.fetchers.SchemaFetcher createSchemaFetcher(String fetcher) {

    switch (fetcher) {
      case "io.stargate.graphql.fetchers.AlterTableAddFetcher":
        return new AlterTableAddFetcher(persistence, authenticationService);
      case "io.stargate.graphql.fetchers.AlterTableDropFetcher":
        return new AlterTableDropFetcher(persistence, authenticationService);
      case "io.stargate.graphql.fetchers.CreateTableDataFetcher":
        return new CreateTableDataFetcher(persistence, authenticationService);
      case "io.stargate.graphql.fetchers.DropTableFetcher":
        return new DropTableFetcher(persistence, authenticationService);
      default:
        throw new IllegalStateException("Unexpected value: " + fetcher);
    }
  }
}
