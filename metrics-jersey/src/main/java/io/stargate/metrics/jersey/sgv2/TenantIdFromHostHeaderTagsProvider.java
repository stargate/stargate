package io.stargate.metrics.jersey.sgv2;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import java.util.Collections;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/** Specialized {@link JerseyTagsProvider} used to extract Tenant Id from "Host" HTTP header. */
public class TenantIdFromHostHeaderTagsProvider implements JerseyTagsProvider {
  private static final String DEFAULT_TENANT_TAG_KEY = "tenant";

  private final String tenantTagName;

  /** Tags when tenant is unknown. */
  private final Tags tagsForUnknown;

  public TenantIdFromHostHeaderTagsProvider() {
    this(DEFAULT_TENANT_TAG_KEY);
  }

  /**
   * @param tenantTagName Tag name to use for Tenant Id, if any; {@code null} or empty String to
   *     disable extraction
   */
  public TenantIdFromHostHeaderTagsProvider(String tenantTagName) {
    if (tenantTagName == null || tenantTagName.isEmpty()) {
      this.tenantTagName = null;
      tagsForUnknown = null;
    } else {
      this.tenantTagName = tenantTagName;
      tagsForUnknown = Tags.of(tenantTagName, "unknown");
    }
  }

  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    return findHostTag(event);
  }

  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    return findHostTag(event);
  }

  protected Iterable<Tag> findHostTag(RequestEvent event) {
    if (tenantTagName == null) {
      return Collections.emptyList();
    }
    ContainerRequest request = event.getContainerRequest();
    MultivaluedMap<String, String> headers = request.getHeaders();

    // HTTP header names are case-insensitive but MultivaluedMap is case-sensitive
    // try out commonly used cases
    String value = headers.getFirst("Host");
    if (value == null) {
      value = headers.getFirst("host");
    }
    if (value != null) {
      if (isValidTenantId(value)) {
        return Tags.of(tenantTagName, trimTenantId(value));
      }
    }
    return tagsForUnknown;
  }

  protected boolean isValidTenantId(String id) {
    // Should we enforce minimum 36-character length?
    return id.length() > 0;
  }

  protected String trimTenantId(String id) {
    return (id.length() <= 36) ? id : id.substring(0, 36);
  }
}
