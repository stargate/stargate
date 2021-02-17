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
package io.stargate.graphql.web.resources.schemafirst;

import java.nio.file.Paths;

public class ResourcePaths {

  /**
   * The path under which all schema-first resource reside. Must not clash with {@code /graphql},
   * which is used by the CQL-first API.
   */
  public static final String ROOT = "/graphqlv2";

  public static final String ADMIN = ROOT + "/admin";
  public static final String NAMESPACES = ROOT + "/namespace";
  public static final String FILES = ROOT + "/files";
  public static final String FILES_RELATIVE_TO_ADMIN =
      Paths.get(ADMIN).relativize(Paths.get(FILES)).toString();
}
