package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Optional;

@ConfigMapping(prefix = "stargate")
public interface StargateConfig {

    /**
     * @return The configuration for {@link io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver} to be used.
     */
    @Valid
    TenantResolverConfig tenantResolver();

    interface TenantResolverConfig {

        /**
         * @return The type of the {@link io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver} used.
         */
        Optional<@Pattern(regexp = "subdomain|custom") String> type();

    }

    /**
     * @return The configuration for {@link io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver} to be used.
     */
    @Valid
    TokenResolverConfig tokenResolver();

    interface TokenResolverConfig {

        /**
         * @return The type of the {@link io.stargate.sgv2.docsapi.api.common.token.CassandraTokenResolver} used.
         */
        Optional<@Pattern(regexp = "header|principal|custom") String> type();

        /**
         * @return Specific settings for the <code>header</code> token resolver type.
         */
        @Valid
        HeaderTokenResolverConfig header();

        interface HeaderTokenResolverConfig {

            @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
            String headerName();

        }

    }

    HeaderBasedAuthConfig headerBasedAuth();

    interface HeaderBasedAuthConfig {

        boolean enabled();

        @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
        String headerName();

    }


    /**
     * @return gRPC Metadata properties for the Bridge.
     */
    @Valid
    StargateConfig.GrpcMetadataConfig grpcMetadata();

    interface GrpcMetadataConfig {

        /**
         * @return Metadata key for passing the tenant-id to the Bridge.
         */
        @NotBlank
        @WithDefault(Constants.TENANT_ID_HEADER_NAME)
        String tenantIdKey();

        /**
         * @return Metadata key for passing the cassandra token to the Bridge.
         */
        @NotBlank
        @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
        String cassandraTokenKey();

    }

    // static constants, not configurable via props

    interface Constants {

        /**
         * Name for the Open API default security scheme.
         */
        String OPEN_API_DEFAULT_SECURITY_SCHEME = "Token";

        /**
         * Authentication token header name.
         */
        String AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

        /**
         * Authentication token header name.
         */
        String TENANT_ID_HEADER_NAME = "X-Tenant-Id";

    }

}
