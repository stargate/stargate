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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.docsapi.config.constants.Constants;

import javax.validation.constraints.NotBlank;

/**
 * Configuration for the header based authentication.
 */
@ConfigMapping(prefix = "stargate.header-based-auth")
public interface HeaderBasedAuthConfig {

    /**
     * @return If the header based auth is enabled.
     */
    boolean enabled();

    /**
     * @return Name of the authentication header. Defaults to {@value Constants#AUTHENTICATION_TOKEN_HEADER_NAME}.
     */
    @NotBlank
    @WithDefault(Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
    String headerName();

}
