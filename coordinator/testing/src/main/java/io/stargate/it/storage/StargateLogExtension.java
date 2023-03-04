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
package io.stargate.it.storage;

import java.lang.reflect.Parameter;
import java.util.UUID;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class StargateLogExtension implements ParameterResolver {

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    return LogCollector.class == parameter.getType();
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    StargateEnvironmentInfo stargate =
        (StargateEnvironmentInfo)
            extensionContext
                .getStore(ExtensionContext.Namespace.GLOBAL)
                .get(StargateExtension.STORE_KEY);

    if (stargate == null) {
      throw new IllegalStateException(
          String.format(
              "%s can only be used in conjunction with %s (make sure it is declared last)",
              getClass().getSimpleName(), StargateExtension.class.getSimpleName()));
    }

    LogCollector collector = new LogCollector(stargate);

    // Store the collector in the extension context to ensure it is closed when the
    // context is destroyed.
    extensionContext.getStore(Namespace.GLOBAL).put(UUID.randomUUID().toString(), collector);

    return collector;
  }
}
