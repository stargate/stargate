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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.osgi.framework.Bundle;

public class PlaygroundServlet extends HttpServlet {

  private final Bundle bundle;

  public PlaygroundServlet(Bundle bundle) {
    this.bundle = bundle;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/html");
    response.setCharacterEncoding("utf-8");
    URL entry = bundle.getEntry("/playground.html");
    // From
    // https://raw.githubusercontent.com/prisma-labs/graphql-playground/master/packages/graphql-playground-html/withAnimation.html
    try (InputStream is = entry.openConnection().getInputStream()) {
      OutputStream os = response.getOutputStream();

      if (is == null) {
        response.setStatus(500);
      } else {
        byte[] buffer = new byte[1024];
        int bytesRead;

        while ((bytesRead = is.read(buffer)) != -1) {
          os.write(buffer, 0, bytesRead);
        }
      }
    } catch (IOException e) {
      response.setStatus(500);
    }
  }
}
