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
package io.stargate.api.sql.server.postgres;

import io.stargate.api.sql.AbstractDataStoreTest;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.datastore.DataStore;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

public abstract class PGServerTestBase extends AbstractDataStoreTest {

  protected static final int TEST_PORT = 5432;

  private static final AtomicReference<DataStore> dataStoreRef = new AtomicReference<>();

  private static PGServer server;

  @BeforeAll
  public static void startServer() {
    AuthenticationService authenticator = Mockito.mock(AuthenticationService.class);
    server = new PGServer(dataStoreRef::get, authenticator, TEST_PORT);
    server.start();
  }

  @AfterAll
  public static void stopServer() throws ExecutionException, InterruptedException {
    server.stop();
  }

  @BeforeEach
  public void setDataStore() {
    dataStoreRef.set(dataStore);
  }
}
