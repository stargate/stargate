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
package io.stargate.sgv2.graphql.web.models;

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.PartType;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

/**
 * Holds the contents of a multipart GraphQL request.
 *
 * <p>Example cURL call:
 *
 * <pre>
 * curl http://host:port/path/to/graphql \
 *   -F operations='{ "query": "query ($file: Upload!) { someQuery(file: $file) }", "variables": { "file": null } };type=application/json'
 *   -F map='{ "filePart": ["variables.file"] }'
 *   -FfilePart=@/path/to/file.txt
 * </pre>
 *
 * @see <a href="https://github.com/jaydenseric/graphql-multipart-request-spec">GraphQL multipart
 *     request specification</a>
 */
public class GraphqlFormData {

  /** The "operations" part contains the GraphQL payload (same as a regular JSON POST). */
  @RestForm
  @PartType(MediaType.APPLICATION_JSON)
  public GraphqlJsonBody operations;

  /** The "map" part indicate how file parts relate to GraphQL variables. */
  @RestForm
  @PartType(MediaType.APPLICATION_JSON)
  public Map<String, List<String>> map;

  /** The file parts. */
  @RestForm(FileUpload.ALL)
  public List<FileUpload> files;
}
