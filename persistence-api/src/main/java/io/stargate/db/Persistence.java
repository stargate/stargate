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
package io.stargate.db;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.utils.MD5Digest;

import io.stargate.db.datastore.DataStore;

public interface Persistence<T,C,Q>
{
    String name();

    void initialize(T config);

    void destroy();

    void registerEventListener(EventListener listener);

    boolean isRpcReady(InetAddressAndPort endpoint);

    InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint);

    QueryState<Q> newQueryState(ClientState<C> clientState);

    ClientState<C> newClientState(SocketAddress remoteAddress, InetSocketAddress publicAddress);

    ClientState newClientState(String name);

    AuthenticatedUser<?> newAuthenticatedUser(String name);

    Authenticator getAuthenticator();

    DataStore newDataStore(QueryState<Q> state, QueryOptions<C> queryOptions);

    CompletableFuture<? extends Result> query(String cql, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime);

    CompletableFuture<? extends Result> execute(MD5Digest id, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime);

    CompletableFuture<? extends Result> prepare(String cql, QueryState state, Map<String, ByteBuffer> customPayload, boolean isTracingRequested);

    CompletableFuture<? extends Result> batch(BatchType type, List<Object> queryOrIds, List<List<ByteBuffer>> values, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime);

    boolean isInSchemaAgreement();

    void captureClientWarnings();

    List<String> getClientWarnings();

    void resetClientWarnings();
}
