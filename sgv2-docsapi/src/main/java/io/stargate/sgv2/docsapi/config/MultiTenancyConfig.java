package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;

/** Configuration mapping for the tenant resolver. */
@ConfigMapping(prefix = "stargate.multi-tenancy")
public interface MultiTenancyConfig {

  /** @return If multi-tenancy is enabled. */
  @WithDefault("false")
  boolean enabled();

  /** @return Tenant resolver in case the multi-tenancy is active. */
  @Valid
  TenantResolverConfig tenantResolver();

  /** Configuration mapping for the tenant resolver. */
  interface TenantResolverConfig {

    /**
     * Tenant resolver type, possible options:
     *
     * <ol>
     *   <li><code>subdomain</code> - reads tenant id from request host domain (see {@link
     *       io.stargate.sgv2.docsapi.api.common.tenant.impl.SubdomainTenantResolver}
     *   <li><code>fixed</code> - fixed tenant id supplied by the configuration (see {@link
     *       io.stargate.sgv2.docsapi.api.common.tenant.impl.FixedTenantResolver}}
     *   <li><code>custom</code> - allows configuring custom tenant resolver
     * </ol>
     *
     * If unset, noop resolver will be used.
     *
     * @return The type of the {@link io.stargate.sgv2.docsapi.api.common.tenant.TenantResolver}
     *     used.
     */
    Optional<@Pattern(regexp = "subdomain|fixed|custom") String> type();

    /** @return Specific settings for the <code>fixed</code> tenant resolver type. */
    @Valid
    MultiTenancyConfig.TenantResolverConfig.FixedTenantResolverConfig fixed();

    interface FixedTenantResolverConfig {

      /** @return Tenant ID value. */
      Optional<String> tenantId();
    }
  }
}
