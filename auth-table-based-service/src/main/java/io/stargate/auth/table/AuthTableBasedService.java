package io.stargate.auth.table;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.WhereCondition;

public class AuthTableBasedService implements AuthenticationService {
    private static final Logger logger = LoggerFactory.getLogger(AuthTableBasedService.class);

    private Persistence persistence;
    private DataStore dataStore;
    private static final String AUTH_KEYSPACE = "data_endpoint_auth";
    private static final String AUTH_TABLE = "token";
    private static final int tokenTTL = Integer.parseInt(System.getProperty("stargate.auth_tokenttl", "1800"));

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
        this.dataStore = persistence.newDataStore(null, null);

        initAuthTable(this.dataStore);
    }

    private void initAuthTable(DataStore dataStore) {
        try {
            dataStore
                    .query(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}", AUTH_KEYSPACE),
                            Optional.of(ConsistencyLevel.LOCAL_QUORUM), Collections.emptyList())
                    .get();
            dataStore
                    .query(String.format("CREATE TABLE IF NOT EXISTS %s.\"%s\" (auth_token UUID, username text, created_timestamp int, PRIMARY KEY (auth_token))", AUTH_KEYSPACE, AUTH_TABLE),
                            Optional.of(ConsistencyLevel.LOCAL_QUORUM), Collections.emptyList())
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
        if (hash == null || hash.isEmpty() || !checkpw(secret, hash)){
            throw new UnauthorizedException(String.format("Provided username %s and/or password are incorrect", key));
        }

        try {
            Instant instant = Instant.now();

            dataStore.query()
                    .insertInto(AUTH_KEYSPACE, AUTH_TABLE)
                    .value("username", key)
                    .value("auth_token", token)
                    .value("created_timestamp", Math.toIntExact(instant.getEpochSecond()))
                    .ttl(tokenTTL)
                    .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .execute();
        } catch (Exception e) {
            logger.error("Failed to add new token", e);
            throw new RuntimeException(e);
        }

        return token.toString();
    }

    private String queryHashedPassword(String key) throws ExecutionException, InterruptedException {
        ResultSet resultSet = dataStore.query()
                .select()
                .column("salted_hash")
                .from("system_auth", "roles")
                .where("role", WhereCondition.Predicate.Eq, key)
                .execute();

        if (resultSet.isEmpty()) {
            throw new RuntimeException(String.format("Provided username %s and/or password are incorrect", key));
        }

        Row row = resultSet.one();
        if (!row.has("salted_hash")) {
            throw new RuntimeException(String.format("Provided username %s and/or password are incorrect", key));
        }

        return row.getString("salted_hash");
    }

    protected static boolean checkpw(String password, String hash)
    {
        try {
            return BCrypt.checkpw(password, hash);
        } catch (Exception e) {
            // Improperly formatted hashes may cause BCrypt.checkpw to throw, so trap any other exception as a failure
            logger.warn("Error: invalid password hash encountered, rejecting user", e);
            return false;
        }
    }

    @Override
    public StoredCredentials validateToken(String token) throws UnauthorizedException {
        if (Strings.isNullOrEmpty(token)) {
            throw new UnauthorizedException("authorization failed, missing token");
        }

        StoredCredentials storedCredentials = new StoredCredentials();
        try {
            ResultSet resultSet = dataStore.query()
                    .select()
                    .star()
                    .from(AUTH_KEYSPACE, AUTH_TABLE)
                    .where("auth_token", WhereCondition.Predicate.Eq, UUID.fromString(token))
                    .execute();

            if (resultSet.isEmpty()) {
                throw new UnauthorizedException("authorization failed");
            }

            Row row = resultSet.one();
            if (!row.has("username")) {
                throw new RuntimeException("unable to get username from token table");
            }

            int timestamp = row.getInt("created_timestamp");
            String username = row.getString("username");

            storedCredentials.setRoleName(username);

            final ResultSet r = dataStore.query()
                    .update(AUTH_KEYSPACE, AUTH_TABLE)
                    .ttl(tokenTTL)
                    .value("username", username)
                    .value("created_timestamp", timestamp)
                    .where("auth_token", WhereCondition.Predicate.Eq, UUID.fromString(token))
                    .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .execute();
        } catch (UnauthorizedException uae) {
            throw uae;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to validate token", e);
            throw new RuntimeException(e);
        }

        return storedCredentials;
    }
}
