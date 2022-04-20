package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.docsapi.config.constants.Constants;
import javax.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;

@OpenAPIDefinition(
    // note that info is defined via the properties
    info = @Info(title = "", version = ""),
    components =
        @Components(
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = Constants.OPEN_API_DEFAULT_SECURITY_SCHEME,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
            }),
    tags = {})
public class StargateDocsApi extends Application {}
