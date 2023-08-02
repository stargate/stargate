package io.stargate.sgv2.restapi.config;

import io.stargate.sgv2.api.common.config.ImmutableRequestParams;
import io.stargate.sgv2.api.common.config.RequestParams;

/** Utility class for the REST API. */
public class RestApiUtils {
  /**
   * Builds the RequestParams object based on the request parameters.
   *
   * @param restApiConfig
   * @param compactMap
   * @return RequestParams
   */
  public static RequestParams getRequestParams(RestApiConfig restApiConfig, Boolean compactMap) {
    final boolean compactMapData = compactMap != null ? compactMap : restApiConfig.compactMapData();
    return ImmutableRequestParams.builder().compactMapData(compactMapData).build();
  }
}
