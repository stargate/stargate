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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin;

import com.google.common.io.CharStreams;
import graphql.schema.DataFetchingEnvironment;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class DeploySchemaFileFetcher extends DeploySchemaFetcherBase {

  @Override
  protected String getSchemaContents(DataFetchingEnvironment environment) throws IOException {
    InputStream file = environment.getArgument("schemaFile");
    return CharStreams.toString(new InputStreamReader(file, StandardCharsets.UTF_8));
  }
}
