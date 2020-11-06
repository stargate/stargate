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
package io.stargate.auth.jwt;

import static io.stargate.auth.jwt.SampleTable.SHOPPING_CART;
import static io.stargate.auth.jwt.SampleTable.SHOPPING_CART_NON_TEXT_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.stargate.auth.TargetCell;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthzJwtServiceTest {

  private AuthzJwtService mockAuthzJwtService;
  private SecretKey key;

  @BeforeEach
  void setup() {
    mockAuthzJwtService = new AuthzJwtService();

    byte[] keyBytes = new byte[32];
    new SecureRandom().nextBytes(keyBytes);
    key = new SecretKeySpec(keyBytes, JWSAlgorithm.RS256.getName());
  }

  @Test
  public void executeDataReadWithAuthorization() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.withRowInspector(any())).thenReturn(resultSet);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<TargetCell> targetCells =
        Collections.singletonList(new TargetCell("userid", "Text", "123"));

    ResultSet result =
        mockAuthzJwtService.authorizedDataRead(action, signJWT(stargate_claims), targetCells);
    assertThat(result.rows().get(0)).isEqualTo(row);
  }

  @Test
  public void executeDataReadWithAuthorizationNullResultSet() throws Exception {
    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(null);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<TargetCell> targetCells =
        Collections.singletonList(new TargetCell("userid", "Text", "123"));

    ResultSet result =
        mockAuthzJwtService.authorizedDataRead(action, signJWT(stargate_claims), targetCells);
    assertThat(result).isNull();
  }

  @Test
  public void executeDataReadWithAuthorizationResultSetWithNoRows() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.withRowInspector(any())).thenReturn(resultSet);
    when(resultSet.rows()).thenReturn(null);

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<TargetCell> targetCells =
        Collections.singletonList(new TargetCell("userid", "Text", "123"));

    ResultSet result =
        mockAuthzJwtService.authorizedDataRead(action, signJWT(stargate_claims), targetCells);
    assertThat(result.rows()).isEqualTo(null);
  }

  @Test
  public void executeDataReadWithAuthorizationMoreValuesThanKeys() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<TargetCell> targetCells =
        Arrays.asList(
            new TargetCell("userid", "Text", "123"), new TargetCell("userid", "Text", "abc"));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mockAuthzJwtService.authorizedDataRead(
                    action, signJWT(stargate_claims), targetCells));
    assertThat(ex).hasMessage("Provided more primary key values than exists");
  }

  @Test
  public void executeDataReadWithAuthorizationNotAuthorized() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "456");

    List<TargetCell> targetCells =
        Collections.singletonList(new TargetCell("userid", "Text", "123"));

    UnauthorizedException ex =
        assertThrows(
            UnauthorizedException.class,
            () ->
                mockAuthzJwtService.authorizedDataRead(
                    action, signJWT(stargate_claims), targetCells));
    assertThat(ex).hasMessage("Not allowed to access this resource");
  }

  @Test
  public void executeDataReadWithAuthorizationNotAuthorizedResult() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "456");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART.columns(), values);
    when(resultSet.withRowInspector(any())).thenReturn(resultSet);
    when(resultSet.rows()).thenReturn(Collections.emptyList());

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");

    List<TargetCell> targetCells =
        Collections.singletonList(new TargetCell("userid", "Text", "123"));

    ResultSet result =
        mockAuthzJwtService.authorizedDataRead(action, signJWT(stargate_claims), targetCells);
    assertThat(result.rows()).isEqualTo(Collections.emptyList());
  }

  @Test
  public void executeDataReadWithAuthorizationClaimOnNonTextColumn() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values = new HashMap<>();
    values.put("userid", "123");
    values.put("item_count", 2);
    values.put("last_update_timestamp", Instant.now());
    Row row = createRow(SHOPPING_CART_NON_TEXT_PK.columns(), values);
    when(resultSet.rows()).thenReturn(Collections.singletonList(row));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "web-user");
    stargate_claims.put("x-stargate-userid", "123");
    stargate_claims.put("x-stargate-item_count", 2);

    List<TargetCell> targetCells =
        Arrays.asList(
            new TargetCell("userid", "Text", "123"), new TargetCell("userid", "Text", "2"));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                mockAuthzJwtService.authorizedDataRead(
                    action, signJWT(stargate_claims), targetCells));
    assertThat(ex).hasMessage("Column must be of type text to be used for authorization");
  }

  @Test
  public void executeDataReadWithAuthorizationNoClaim() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    Map<String, Object> values1 = new HashMap<>();
    values1.put("userid", "123");
    values1.put("item_count", 2);
    values1.put("last_update_timestamp", Instant.now());
    Row row1 = createRow(SHOPPING_CART.columns(), values1);

    Map<String, Object> values2 = new HashMap<>();
    values2.put("userid", "456");
    values2.put("item_count", 4);
    values2.put("last_update_timestamp", Instant.now());
    Row row2 = createRow(SHOPPING_CART.columns(), values2);

    when(resultSet.withRowInspector(any())).thenReturn(resultSet);
    when(resultSet.rows()).thenReturn(Arrays.asList(row1, row2));

    Callable<ResultSet> action = mock(Callable.class);
    when(action.call()).thenReturn(resultSet);

    Map<String, Object> stargate_claims = new HashMap<>();
    stargate_claims.put("x-stargate-role", "admin");

    List<TargetCell> targetCells = Collections.emptyList();

    ResultSet result =
        mockAuthzJwtService.authorizedDataRead(action, signJWT(stargate_claims), targetCells);
    assertThat(result.rows().get(0)).isEqualTo(row1);
    assertThat(result.rows().get(1)).isEqualTo(row2);
  }

  private String signJWT(Map<String, Object> stargate_claims) throws JOSEException {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder().claim("stargate_claims", stargate_claims).build();

    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    jwt.sign(new MACSigner(key));

    return jwt.serialize();
  }

  private Row createRow(List<Column> columns, Map<String, Object> data) {
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    for (Column column : columns) {
      Object v = data.get(column.name());
      values.add(v == null ? null : column.type().codec().encode(v, ProtocolVersion.DEFAULT));
    }
    return new ArrayListBackedRow(columns, values, ProtocolVersion.DEFAULT);
  }
}
