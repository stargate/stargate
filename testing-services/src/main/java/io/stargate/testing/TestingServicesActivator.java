/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.testing;

import io.stargate.auth.AuthorizationProcessor;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.testing.auth.LoggingAuthorizationProcessorImpl;
import io.stargate.testing.metrics.TagMeHttpMetricsTagProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class TestingServicesActivator extends BaseActivator {

  public static final String AUTHZ_PROCESSOR_PROPERTY = "stargate.authorization.processor.id";
  public static final String LOGGING_AUTHZ_PROCESSOR_ID = "LoggingAuthzProcessor";

  public static final String HTTP_TAG_PROVIDER_PROPERTY = "stargate.metrics.http_tag_provider.id";
  public static final String TAG_ME_HTTP_TAG_PROVIDER = "TagMeProvider";

  public TestingServicesActivator() {
    super("testing-services");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    List<ServiceAndProperties> services = new ArrayList<>();

    if (LOGGING_AUTHZ_PROCESSOR_ID.equals(System.getProperty(AUTHZ_PROCESSOR_PROPERTY))) {
      LoggingAuthorizationProcessorImpl authzProcessor = new LoggingAuthorizationProcessorImpl();

      Hashtable<String, String> props = new Hashtable<>();
      props.put("AuthProcessorId", LOGGING_AUTHZ_PROCESSOR_ID);

      services.add(new ServiceAndProperties(authzProcessor, AuthorizationProcessor.class, props));
    }

    if (TAG_ME_HTTP_TAG_PROVIDER.equals(System.getProperty(HTTP_TAG_PROVIDER_PROPERTY))) {
      TagMeHttpMetricsTagProvider tagProvider = new TagMeHttpMetricsTagProvider();

      services.add(new ServiceAndProperties(tagProvider, HttpMetricsTagProvider.class, null));
    }

    return services;
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
