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
package io.stargate.graphql.schema.schemafirst.processor;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.ErrorClassification;
import graphql.GraphQLError;
import graphql.language.SourceLocation;
import java.util.Collections;
import java.util.List;

/**
 * Custom GraphQL error implementation to surface schema processing issues.
 *
 * <p>Note that we also reuse this for info/warning logs: they're not errors per se, but since they
 * share a similar structure it's convenient to factor the code.
 */
public class ProcessingMessage implements GraphQLError {

  private final String message;
  private final List<SourceLocation> locations;
  private final ErrorClassification errorType;

  @FormatMethod
  public ProcessingMessage(
      SourceLocation location,
      ErrorClassification errorType,
      @FormatString String format,
      Object... arguments) {
    this.errorType = errorType;
    this.message = String.format(format, arguments);
    this.locations = Collections.singletonList(location);
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public List<SourceLocation> getLocations() {
    return locations;
  }

  @Override
  public ErrorClassification getErrorType() {
    return errorType;
  }
}
