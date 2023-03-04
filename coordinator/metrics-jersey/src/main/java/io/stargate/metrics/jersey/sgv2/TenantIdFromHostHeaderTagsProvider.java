package io.stargate.metrics.jersey.sgv2;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import java.util.Objects;
import java.util.Optional;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * Specialized {@link JerseyTagsProvider} used to extract Tenant Id from "Host" HTTP header.
 *
 * <p>NOTE: there is slight duplication with this class and {@code CreateStargateBridgeClientFilter}
 * in "sgv2-service-common" as both extract "tenant" information from "Host" header. When converting
 * to the next-gen framework (probably Quarkus) we can hopefully merge or extract shared logic (it's
 * not a lot of code but should be unified).
 */
public class TenantIdFromHostHeaderTagsProvider implements JerseyTagsProvider {
  private static final String DEFAULT_TENANT_TAG_KEY = "tenant";

  private final String tenantTagName;

  /** Tags when tenant is unknown. */
  private final Tags tagsForUnknown;

  public TenantIdFromHostHeaderTagsProvider() {
    this(DEFAULT_TENANT_TAG_KEY);
  }

  /** @param tenantTagName Tag name to use for Tenant Id */
  public TenantIdFromHostHeaderTagsProvider(String tenantTagName) {
    this.tenantTagName = Objects.requireNonNull(tenantTagName);
    tagsForUnknown = Tags.of(tenantTagName, "unknown");
  }

  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    return extractTenantFromHost(event);
  }

  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    return extractTenantFromHost(event);
  }

  protected Iterable<Tag> extractTenantFromHost(RequestEvent event) {
    ContainerRequest request = event.getContainerRequest();
    MultivaluedMap<String, String> headers = request.getHeaders();

    // HTTP header names are case-insensitive but MultivaluedMap is case-sensitive
    // try out commonly used cases
    String value = headers.getFirst("Host");
    if (value == null) {
      value = headers.getFirst("host");
    }
    return extractTenantId(value).map(t -> Tags.of(tenantTagName, t)).orElse(tagsForUnknown);
  }

  protected Optional<String> extractTenantId(String host) {
    if (host == null || host.length() < 36) {
      return Optional.empty();
    }
    // Could further check structure with regex but seems like length is enough
    // to weed out most invalid cases. Can add regex if necessary.
    return Optional.of(host.substring(0, 36));
  }
}
