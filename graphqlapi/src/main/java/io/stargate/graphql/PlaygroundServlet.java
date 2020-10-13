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
package io.stargate.graphql;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.osgi.framework.Bundle;

public class PlaygroundServlet extends HttpServlet {

  private final String playgroundFile;

  public PlaygroundServlet(Bundle bundle) throws IOException {
    // From
    // https://raw.githubusercontent.com/prisma-labs/graphql-playground/master/packages/graphql-playground-html/withAnimation.html
    URL entry = bundle.getEntry("/playground.html");
    // Save the templated file away for later so that we only have to do this conversion once.
    playgroundFile =
        new BufferedReader(new InputStreamReader(entry.openConnection().getInputStream()))
            .lines()
            .collect(Collectors.joining("\n"));
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/html");
    response.setCharacterEncoding("utf-8");

    String token = request.getHeader("x-cassandra-token");

    // Replace the templated text with the token if it exist. Using java.lang.String.replaceFirst
    // since it's safer than java.lang.String.format(java.lang.String, java.lang.Object...) due to
    // the percent signs that exist in the string.
    String formattedIndexFile =
        playgroundFile.replaceFirst("AUTHENTICATION_TOKEN", token == null ? "" : token);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(formattedIndexFile.getBytes());
    try {
      OutputStream os = response.getOutputStream();

      byte[] buffer = new byte[1024];
      int bytesRead;

      while ((bytesRead = byteArrayInputStream.read(buffer)) != -1) {
        os.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      response.setStatus(500);
    }
  }
}
