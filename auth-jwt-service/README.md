# JWT Authn/z Service for Stargate

Service capable of providing JWT based authentication and authorization to other Stargate modules.

## Local Quickstart

To get started locally you can run a local instance of [Keycloak](https://www.keycloak.org/) with
the provided realm in [stargate-realm.json](../testing/src/test/resources/stargate-realm.json) which has 
a client set up with mappings to populate JWTs with the necessary claims. 

```sh
docker run \
  -it \
  --name keycloak \
  --rm \
  -e KEYCLOAK_USER=admin \
  -e KEYCLOAK_PASSWORD=admin \
  -e KEYCLOAK_IMPORT=/tmp/stargate-realm.json \
  -v $PWD/stargate-realm.json:/tmp/stargate-realm.json \
  -p 4444:4444 \
  quay.io/keycloak/keycloak:11.0.2 -Djboss.http.port=4444
```

Once Keycloak is up you can add a new user

```shell
TOKEN=$(curl -s --data "username=admin&password=admin&grant_type=password&client_id=admin-cli" http://localhost:4444/auth/realms/master/protocol/openid-connect/token | jq -r '.access_token')

curl -L -X POST 'http://localhost:4444/auth/admin/realms/stargate/users' \
-H "Content-Type: application/json" \
-H "Authorization: bearer $TOKEN" \
--data-raw '{
    "username": "testuser1",
    "enabled": true,
    "emailVerified": true,
    "attributes": {
        "userid": [
            "9876"
        ],
        "role": [
            "web_user"
        ]
    },
    "credentials": [
        {
            "type": "password",
            "value": "testuser1",
            "temporary": "false"
        }
    ]
}'
```

Next start up Stargate, configuring the new JWT auth service 

```sh
./mvnw clean package -DskipTests && \
JAVA_OPTS='-XX:+CrashOnOutOfMemoryError -Xmx750M -Xms64M -Dstargate.auth_id=AuthJwtService -Dstargate.auth.jwt_provider_url=http://localhost:4444/auth/realms/stargate/protocol/openid-connect/certs' \
./starctl --developer-mode --cluster-name test --cluster-version 3.11 --enable-auth
```


Once Stargate is up and running some basic test data can be added.

```cql
CREATE ROLE IF NOT EXISTS 'web_user' WITH PASSWORD = 'web_user' AND LOGIN = TRUE;

CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};

CREATE TABLE IF NOT EXISTS store.shopping_cart (userid text PRIMARY KEY, item_count int, last_update_timestamp timestamp);

INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(toDate(now())));
INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(toDate(now())));
GRANT MODIFY ON TABLE store.shopping_cart TO web_user;
GRANT SELECT ON TABLE store.shopping_cart TO web_user;
```

Finally, issue a few curl commands to make sure everything is working as expected.

```sh
USER_TOKEN=$(curl -s --data "username=testuser1&password=testuser1&grant_type=password&client_id=user-service" http://localhost:4444/auth/realms/stargate/protocol/openid-connect/token | jq -r '.access_token')

curl -sL 'localhost:8082/v1/keyspaces/system/tables/local/rows/local' \
-H "X-Cassandra-Token: $USER_TOKEN" | jq .

curl -sL 'localhost:8082/v1/keyspaces/store/tables/shopping_cart/rows/9876' \
-H "X-Cassandra-Token: $USER_TOKEN" | jq .
{
  "count": 1,
  "rows": [
    {
      "item_count": 2,
      "userid": "9876",
      "last_update_timestamp": "2020-11-06T00:00:00Z"
    }
  ]
}

# Shouldn't be able to access a record for another user
curl -sL 'localhost:8082/v1/keyspaces/store/tables/shopping_cart/rows/1234' \
-H "X-Cassandra-Token: $USER_TOKEN" | jq .
{
  "description": "Role unauthorized for operation: Not allowed to access this resource",
  "code": 401
}
```
