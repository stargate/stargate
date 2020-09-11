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
package io.stargate.auth.api;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;

import io.stargate.auth.model.Error;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ErrorHandler extends ErrorPageErrorHandler {

    private static final String ERROR_404_MESSAGE = "Target resource not found";
    private static final String ERROR_501_MESSAGE = "Server functionality to process request is not implemented";
    private static final String ERROR_502_MESSAGE = "Server cannot proxy request";
    private static final String ERROR_503_MESSAGE = "Server is currently unable to handle the request";
    private static final String ERROR_504_MESSAGE = "Server did not receive a timely response from an upstream server";
    private static final String ERROR_UNEXPECTED_MESSAGE = "Unexpected error occurs";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    protected void generateAcceptableResponse(Request baseRequest, HttpServletRequest request, HttpServletResponse response,
                                              int code, String message, String mimeType) throws IOException {
        baseRequest.setHandled(true);
        Writer writer = getAcceptableWriter(baseRequest, request, response);

        if (null != writer) {
            response.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
            response.setStatus(code);
            handleErrorPage(request, writer, code, message);
        }
    }

    @Override
    protected Writer getAcceptableWriter(Request baseRequest, HttpServletRequest request,
                                         HttpServletResponse response) throws IOException {
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        return response.getWriter();
    }

    @Override
    protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String message,
                                  boolean showStacks) throws IOException {
        // TODO: [doug] 2020-06-18, Thu, 0:45 populate internalCode 
        Error error = new Error().description(getMessage(code));
        writer.write(MAPPER.writeValueAsString(error));
    }

    private String getMessage(int code) {
        switch (code) {
            case 404:
                return ERROR_404_MESSAGE;
            case 501:
                return ERROR_501_MESSAGE;
            case 502:
                return ERROR_502_MESSAGE;
            case 503:
                return ERROR_503_MESSAGE;
            case 504:
                return ERROR_504_MESSAGE;
            default:
                return ERROR_UNEXPECTED_MESSAGE;
        }
    }
}
