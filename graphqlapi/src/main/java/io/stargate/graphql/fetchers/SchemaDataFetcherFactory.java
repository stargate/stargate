/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
