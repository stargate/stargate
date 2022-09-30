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
package io.stargate.sgv2.graphql.schema;

import static graphql.schema.GraphQLScalarType.newScalar;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import io.stargate.sgv2.graphql.web.models.GraphqlFormData;
import java.io.InputStream;

public class FileSupport {

  /**
   * A custom scalar that can be used to map file uploads in GraphQL schemas.
   *
   * @see io.stargate.sgv2.graphql.web.resources.GraphqlResourceBase
   * @see GraphqlFormData
   */
  public static final GraphQLScalarType UPLOAD_SCALAR =
      newScalar().name("Upload").coercing(new InputStreamCoercing()).build();

  private static class InputStreamCoercing implements Coercing<InputStream, Void> {
    @Override
    public Void serialize(Object dataFetcherResult) throws CoercingSerializeException {
      throw new CoercingSerializeException("Upload can only be used for input");
    }

    @Override
    public InputStream parseValue(Object input) throws CoercingParseValueException {
      if (input instanceof InputStream) {
        return ((InputStream) input);
      } else {
        throw new CoercingParseValueException(
            String.format(
                "Expected a FileInputStream, got %s",
                input == null ? "null" : input.getClass().getSimpleName()));
      }
    }

    @Override
    public InputStream parseLiteral(Object input) throws CoercingParseLiteralException {
      throw new CoercingParseLiteralException("Upload values must be specified as JSON variables");
    }
  }
}
