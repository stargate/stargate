package io.stargate.sgv2.restsvc.resources;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.stargate.proto.QueryOuterClass;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import java.util.Base64;
import javax.ws.rs.core.Response;

/**
 * Base class for resource classes; contains utility/helper methods for which there is no more
 * specific place.
 */
public abstract class ResourceBase {

  // // // Helper methods for Response construction

  protected static Response invalidTokenFailure() {
    return jaxrsResponse(Response.Status.UNAUTHORIZED)
        .entity(
            new RestServiceError(
                "Missing or invalid Auth Token", Response.Status.UNAUTHORIZED.getStatusCode()))
        .build();
  }

  protected static Response.ResponseBuilder jaxrsResponse(Response.Status status) {
    return Response.status(status);
  }

  // // // Helper methods for gRPC type manipulation

  protected static String extractPagingStateFromResultSet(QueryOuterClass.ResultSet rs) {
    BytesValue pagingStateOut = rs.getPagingState();
    if (pagingStateOut.isInitialized()) {
      ByteString rawPS = pagingStateOut.getValue();
      if (!rawPS.isEmpty()) {
        byte[] b = rawPS.toByteArray();
        // Could almost use "ByteBufferUtils.toBase64" but need variant that takes 'byte[]'
        return Base64.getEncoder().encodeToString(b);
      }
    }
    return null;
  }

  // // // Helper methods for input validation

  /**
   * Method that checks for some common types of invalidity for Auth Token: currently simply its
   * existence (cannot be {@code null}) or empty ({code !string.isEmpty()}), but may be extended
   * with heuristics in future.
   */
  protected static final boolean isAuthTokenInvalid(String authToken) {
    return isStringEmpty(authToken);
  }

  protected static final boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }
}
