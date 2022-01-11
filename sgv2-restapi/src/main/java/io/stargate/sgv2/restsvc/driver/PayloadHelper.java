package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PayloadHelper {

  // TODO shared with PersistenceConnectionFactory in the cql module, maybe find a way to factor
  private static final String TOKEN_PAYLOAD_KEY = "stargate.bridge.token";
  private static final String TENANT_PAYLOAD_KEY = "stargate.bridge.tenantId";

  static Map<String, ByteBuffer> buildExtraPayload(String token, String tenantId) {
    assert token != null || tenantId != null;
    Map<String, ByteBuffer> payload = new HashMap<>();
    if (token != null) {
      payload.put(TOKEN_PAYLOAD_KEY, StandardCharsets.UTF_8.encode(token));
    }
    if (tenantId != null) {
      payload.put(TENANT_PAYLOAD_KEY, StandardCharsets.UTF_8.encode(tenantId));
    }
    return Collections.unmodifiableMap(payload);
  }

  static <RequestT extends Request> RequestT augment(
      RequestT request, Map<String, ByteBuffer> extraPayload) {
    if (request instanceof Statement) {
      Statement<?> statement = (Statement<?>) request;
      Map<String, ByteBuffer> newPayload = augment(statement.getCustomPayload(), extraPayload);
      statement = statement.setCustomPayload(newPayload);
      // The cast is safe because setCustomPayload returns the statement's self type
      @SuppressWarnings("unchecked")
      RequestT newRequest = (RequestT) statement;
      return newRequest;
    } else if (request instanceof PrepareRequest) {
      PrepareRequest prepare = (PrepareRequest) request;
      Map<String, ByteBuffer> newPayload = augment(prepare.getCustomPayload(), extraPayload);
      Map<String, ByteBuffer> newPayloadForBoundStatements =
          augment(prepare.getCustomPayloadForBoundStatements(), extraPayload);
      @SuppressWarnings("unchecked")
      RequestT newRequest =
          (RequestT) new ScopedPrepareRequest(prepare, newPayload, newPayloadForBoundStatements);
      return newRequest;
    } else {
      // That's all we should encounter for CQL
      throw new UnsupportedOperationException(
          "Unexpected request type " + request.getClass().getName());
    }
  }

  private static Map<String, ByteBuffer> augment(
      Map<String, ByteBuffer> payload, Map<String, ByteBuffer> toAdd) {
    if (payload == null || payload.isEmpty()) {
      return toAdd;
    } else {
      try {
        payload.putAll(toAdd);
        return payload;
      } catch (UnsupportedOperationException e) {
        Map<String, ByteBuffer> newPayload = new HashMap<>(payload);
        newPayload.putAll(toAdd);
        return newPayload;
      }
    }
  }

  private PayloadHelper() {
    // intentionally empty
  }
}
