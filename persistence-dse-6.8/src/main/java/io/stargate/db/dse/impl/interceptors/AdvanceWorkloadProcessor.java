package io.stargate.db.dse.impl.interceptors;

import io.reactivex.Single;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.nio.ByteBuffer;
import java.util.Map;

public interface AdvanceWorkloadProcessor {
    default Single<ResultMessage> process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime){
        return null;
    }
}
