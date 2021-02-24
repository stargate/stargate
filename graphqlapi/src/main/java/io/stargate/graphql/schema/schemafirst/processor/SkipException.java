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

/**
 * Used by mapping model classes to fail fast when an element has mapping errors. The caller must
 * report errors itself (with {@link ProcessingContext#addError}) before throwing. Note that {@link
 * MappingModel#build} will fail at the end if any errors have been reported, so it's fine to
 * swallow this exception and keep going if the code wants to report as many errors as possible.
 *
 * <p>This exception is used for flow control only, it never surfaces to the client directly.
 */
class SkipException extends Exception {

  static SkipException INSTANCE = new SkipException();

  private SkipException() {
    super(null, null, false, false);
  }
}
