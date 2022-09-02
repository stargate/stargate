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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import com.google.errorprone.annotations.FormatMethod;
import graphql.language.SourceLocation;

abstract class ModelBuilderBase<ModelT> {

  protected final ProcessingContext context;
  protected final SourceLocation location;

  protected ModelBuilderBase(ProcessingContext context, SourceLocation location) {
    this.context = context;
    this.location = location;
  }

  abstract ModelT build() throws SkipException;

  @FormatMethod
  protected void info(String format, Object... arguments) {
    context.addInfo(location, format, arguments);
  }

  @FormatMethod
  protected void warn(String format, Object... arguments) {
    context.addWarning(location, format, arguments);
  }

  @FormatMethod
  public void invalidMapping(String format, Object... arguments) {
    context.addError(location, ProcessingErrorType.InvalidMapping, format, arguments);
  }

  @FormatMethod
  protected void invalidSyntax(String format, Object... arguments) {
    context.addError(location, ProcessingErrorType.InvalidSyntax, format, arguments);
  }
}
