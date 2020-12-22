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
package io.stargate.auth.table;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Authenticator.SaslNegotiator;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthnTableBasedService implements AuthenticationService {

  private static final Logger logger = LoggerFactory.getLogger(AuthnTableBasedService.class);

  private DataStore dataStore;
  private static final String AUTH_KEYSPACE =
      System.getProperty("stargate.auth_keyspace", "data_endpoint_auth");
  private static final String AUTH_TABLE = System.getProperty("stargate.auth_table", "token");
  private static final int tokenTTL =
      Integer.parseInt(System.getProperty("stargate.auth_tokenttl", "1800"));
  private static final boolean shouldInitializeAuthKeyspace =
      Boolean.parseBoolean(System.getProperty("stargate.auth_tablebased_init", "true"));

  public void setDataStoreFactory(DataStoreFactory dataStoreFactory) {
    this.dataStore = dataStoreFactory.create();

    if (shouldInitializeAuthKeyspace) {
      initAuthTable(this.dataStore);
    }
  }

  private void initAuthTable(DataStore dataStore) {
    try {
      logger.info(
          "Initializing keyspace {} and table {} for table based auth", AUTH_KEYSPACE, AUTH_TABLE);

      dataStore
          .queryBuilder()
          .create()
          .keyspace(AUTH_KEYSPACE)
          .ifNotExists()
          .withReplication(Replication.simpleStrategy(1))
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();

      dataStore
          .queryBuilder()
          .create()
          .table(AUTH_KEYSPACE, AUTH_TABLE)
          .ifNotExists()
          .column("auth_token", Type.Uuid, Kind.PartitionKey)
          .column("username", Type.Text)
          .column("created_timestamp", Type.Int)
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();
    } catch (Exception e) {
      logger.error("Failed to initialize auth table", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String createToken(String key, String secret) throws UnauthorizedException {
    UUID token = UUID.randomUUID();

    String hash;
    try {
      hash = queryHashedPassword(key);
    } catch (Exception e) {
      throw new UnauthorizedException(e.getMessage());
    }
    if (hash == null || hash.isEmpty() || !checkpw(secret, hash)) {
      throw new UnauthorizedException(
          String.format("Provided username %s and/or password are incorrect", key));
    }

    saveToken(key, token);

    return token.toString();
  }

  @Override
  public String createToken(String key) throws UnauthorizedException {
    UUID token = UUID.randomUUID();

    String username;
    try {
      username = queryUsername(key);
    } catch (Exception e) {
      throw new UnauthorizedException(e.getMessage());
    }

    if (username == null || username.isEmpty()) {
      throw new UnauthorizedException(
          String.format("Provided username %s and/or password are incorrect", key));
    }

    saveToken(key, token);

    return token.toString();
  }

  private void saveToken(String key, UUID token) {
    try {
      Instant instant = Instant.now();

      dataStore
          .queryBuilder()
          .insertInto(AUTH_KEYSPACE, AUTH_TABLE)
          .value("username", key)
          .value("auth_token", token)
          .value("created_timestamp", Math.toIntExact(instant.getEpochSecond()))
          .ttl(tokenTTL)
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();

    } catch (Exception e) {
      logger.error("Failed to add new token", e);
      throw new RuntimeException(e);
    }
  }

  private String queryUsername(String key) throws ExecutionException, InterruptedException {
    ResultSet resultSet =
        dataStore
            .queryBuilder()
            .select()
            .column("role")
            .from("system_auth", "roles")
            .where("role", Predicate.EQ, key)
            .build()
            .execute()
            .get();

    if (resultSet.hasNoMoreFetchedRows()) {
      throw new RuntimeException(String.format("Provided username %s is incorrect", key));
    }

    Row row = resultSet.one();
    if (row.isNull("role")) {
      throw new RuntimeException(String.format("Provided username %s is incorrect", key));
    }

    return row.getString("role");
  }

  private String queryHashedPassword(String key) throws ExecutionException, InterruptedException {
    ResultSet resultSet =
        dataStore
            .queryBuilder()
            .select()
            .column("salted_hash")
            .from("system_auth", "roles")
            .where("role", Predicate.EQ, key)
            .build()
            .execute()
            .get();

    if (resultSet.hasNoMoreFetchedRows()) {
      throw new RuntimeException(
          String.format("Provided username %s and/or password are incorrect", key));
    }

    Row row = resultSet.one();
    if (row.isNull("salted_hash")) {
      throw new RuntimeException(
          String.format("Provided username %s and/or password are incorrect", key));
    }

    return row.getString("salted_hash");
  }

  protected static boolean checkpw(String password, String hash) {
    try {
      return BCrypt.checkpw(password, hash);
    } catch (Exception e) {
      // Improperly formatted hashes may cause BCrypt.checkpw to throw, so trap any other exception
      // as a failure
      logger.warn("Error: invalid password hash encountered, rejecting user", e);
      return false;
    }
  }

  @Override
  public StoredCredentials validateToken(String token) throws UnauthorizedException {
    UUID uuid = parseToken(token);

    try {
      ResultSet resultSet = queryAuthTable(uuid).get();
      Row row = extractCredentialsRow(resultSet);
      updateAuthTableTtl(row).get();
      return new StoredCredentials().roleName(row.getString("username"));
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Failed to validate token", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public CompletionStage<StoredCredentials> validateTokenAsync(String token) {
    return CompletableFuture.completedFuture(token)
        .thenApply(this::parseToken)
        .thenCompose(this::queryAuthTable)
        .thenApply(this::extractCredentialsRow)
        .thenCompose(
            row ->
                updateAuthTableTtl(row)
                    .thenApply(__ -> new StoredCredentials().roleName(row.getString("username"))));
  }

  private UUID parseToken(String token) throws UnauthorizedException {
    if (Strings.isNullOrEmpty(token)) {
      throw new UnauthorizedException("authorization failed - missing token");
    }
    try {
      return UUID.fromString(token);
    } catch (IllegalArgumentException exception) {
      throw new UnauthorizedException("authorization failed - bad token");
    }
  }

  private CompletableFuture<ResultSet> queryAuthTable(UUID uuid) {
    return dataStore
        .queryBuilder()
        .select()
        .star()
        .from(AUTH_KEYSPACE, AUTH_TABLE)
        .where("auth_token", Predicate.EQ, uuid)
        .build()
        .execute();
  }

  private Row extractCredentialsRow(ResultSet resultSet) throws UnauthorizedException {
    if (resultSet.hasNoMoreFetchedRows()) {
      throw new UnauthorizedException("authorization failed");
    }

    Row row = resultSet.one();
    if (row.isNull("username")) {
      throw new RuntimeException("unable to get username from token table");
    }
    return row;
  }

  private CompletableFuture<ResultSet> updateAuthTableTtl(Row row) {
    return dataStore
        .queryBuilder()
        .update(AUTH_KEYSPACE, AUTH_TABLE)
        .ttl(tokenTTL)
        .value("username", row.getString("username"))
        .value("created_timestamp", row.getInt("created_timestamp"))
        .where("auth_token", Predicate.EQ, row.getUuid("auth_token"))
        .build()
        .execute(ConsistencyLevel.LOCAL_QUORUM);
  }

  @Override
  public SaslNegotiator getSaslNegotiator(SaslNegotiator wrapped) {
    return new PlainTextTableBasedTokenSaslNegotiator(
        this,
        wrapped,
        System.getProperty("stargate.cql_token_username", "token"),
        Integer.parseInt(System.getProperty("stargate.cql_token_max_length", "36")));
  }
}
