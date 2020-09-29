/*
 * Copyright © 2014 Federico Recio (N/A)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (C) 2014 Federico Recio
/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.swagger;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import io.dropwizard.views.freemarker.FreemarkerViewRenderer;
import io.federecio.dropwizard.swagger.AuthParamFilter;
import io.federecio.dropwizard.swagger.ConfigurationHelper;
import io.federecio.dropwizard.swagger.SwaggerResource;
import io.federecio.dropwizard.swagger.SwaggerViewConfiguration;
import io.swagger.config.FilterFactory;
import io.swagger.converter.ModelConverters;
import io.swagger.jackson.ModelResolver;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import java.util.Collections;

/**
 * A {@link io.dropwizard.ConfiguredBundle} that provides hassle-free configuration of Swagger and
 * Swagger UI on top of Dropwizard.
 */
public abstract class SwaggerBundle<T extends Configuration> implements ConfiguredBundle<T> {

  @Override
  public void initialize(Bootstrap<?> bootstrap) {
    bootstrap.addBundle(
        new ViewBundle<Configuration>(
            Collections.singletonList(
                new FreemarkerViewRenderer(
                    freemarker.template.Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS))));
    ModelConverters.getInstance().addConverter(new ModelResolver(bootstrap.getObjectMapper()));
  }

  @Override
  public void run(T configuration, Environment environment) throws Exception {
    final SwaggerBundleConfiguration swaggerBundleConfiguration =
        getSwaggerBundleConfiguration(configuration);
    if (swaggerBundleConfiguration == null) {
      throw new IllegalStateException(
          "You need to provide an instance of SwaggerBundleConfiguration");
    }

    if (!swaggerBundleConfiguration.isEnabled()) {
      return;
    }

    final ConfigurationHelper configurationHelper =
        new ConfigurationHelper(configuration, swaggerBundleConfiguration);

    swaggerBundleConfiguration.build(configurationHelper.getUrlPattern());

    FilterFactory.setFilter(new AuthParamFilter());

    // Using .class to avoid context being null per
    // https://github.com/smoketurner/dropwizard-swagger/issues/201
    environment.jersey().register(ApiListingResource.class);
    environment.jersey().register(new SwaggerSerializers());
    if (swaggerBundleConfiguration.isIncludeSwaggerResource()) {
      SwaggerViewConfiguration swaggerViewConfiguration =
          swaggerBundleConfiguration.getSwaggerViewConfiguration();
      SwaggerResource swaggerResource =
          new SwaggerResource(
              configurationHelper.getUrlPattern(),
              swaggerViewConfiguration,
              swaggerBundleConfiguration.getSwaggerOAuth2Configuration(),
              swaggerBundleConfiguration.getContextRoot(),
              swaggerBundleConfiguration.getCustomJavascript());

      // TODO: [doug] 2020-09-29, Tue, 17:18 is the tccl needed here or can we use default?
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      environment.jersey().register(swaggerResource);
      Thread.currentThread().setContextClassLoader(tccl);
    }
  }

  protected abstract SwaggerBundleConfiguration getSwaggerBundleConfiguration(T configuration);
}
