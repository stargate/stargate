package io.stargate.sgv2.docsapi.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;

@StaticInitSafe
@ConfigMapping(prefix = "stargate")
public interface StargateConfig {

    interface Constants {

        /**
         * Name for the Open API default security scheme.
         */
        String OPEN_API_DEFAULT_SECURITY_SCHEME = "Token";

        /**
         * Authentication token header name.
         */
        String AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

    }

}
