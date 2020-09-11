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
