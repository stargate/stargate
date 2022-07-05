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
package io.stargate.sgv2.graphql.schema.scalars;

import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.sgv2.api.common.util.ByteBufferUtils;
import io.stargate.sgv2.graphql.schema.Uuids;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Handles type conversions for custom scalars that can only be represented by strings in GraphQL or
 * JSON payloads.
 *
 * @param <DriverTypeT> how the Java driver represents this type. This is what we'll deserialize to
 *     for incoming data, and will send back for results.
 */
abstract class StringCoercing<DriverTypeT> implements Coercing<DriverTypeT, String> {

  protected abstract String format(DriverTypeT value);

  protected abstract DriverTypeT parse(String value);

  @Override
  public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
    // The cast will always succeed since we've generated the GraphQL field type based on the CQL
    // column type
    @SuppressWarnings("unchecked")
    DriverTypeT value = (DriverTypeT) dataFetcherResult;
    return format(value);
  }

  @Override
  public DriverTypeT parseValue(Object input) throws CoercingParseValueException {
    if (!(input instanceof String)) {
      throw new CoercingParseLiteralException("Expected a string literal");
    }
    try {
      return parse((String) input);
    } catch (CoercingParseLiteralException e) {
      throw e;
    } catch (Exception e) {
      throw new CoercingParseLiteralException(e.getMessage(), e);
    }
  }

  @Override
  public DriverTypeT parseLiteral(Object input) throws CoercingParseLiteralException {
    if (!(input instanceof StringValue)) {
      throw new CoercingParseLiteralException("Expected a string literal");
    }
    try {
      return parse(((StringValue) input).getValue());
    } catch (CoercingParseLiteralException e) {
      throw e;
    } catch (Exception e) {
      throw new CoercingParseLiteralException(e.getMessage(), e);
    }
  }

  static final StringCoercing<java.util.UUID> UUID =
      new StringCoercing<UUID>() {
        @Override
        protected String format(java.util.UUID value) {
          return value.toString();
        }

        @Override
        protected java.util.UUID parse(String value) {
          if (value.equals("uuid()")) {
            return java.util.UUID.randomUUID();
          }
          return java.util.UUID.fromString(value);
        }
      };

  static final StringCoercing<UUID> TIMEUUID =
      new StringCoercing<UUID>() {
        @Override
        protected String format(java.util.UUID value) {
          return value.toString();
        }

        @Override
        protected java.util.UUID parse(String value) {
          if (value.equals("now()")) {
            return Uuids.timeBased();
          }

          java.util.UUID uuid = java.util.UUID.fromString(value);
          if (uuid.version() != 1) {
            throw new CoercingParseLiteralException("Not a Type 1 (time-based) UUID");
          }
          return uuid;
        }
      };

  static final StringCoercing<InetAddress> INET =
      new StringCoercing<InetAddress>() {
        @Override
        protected String format(InetAddress value) {
          return value.getHostAddress();
        }

        @Override
        protected InetAddress parse(String value) {
          try {
            return InetAddress.getByName(String.valueOf(value));
          } catch (UnknownHostException e) {
            throw new CoercingParseLiteralException("Unable to parse Inet: " + e.getMessage());
          }
        }
      };

  static final StringCoercing<String> ASCII =
      new StringCoercing<String>() {
        private final Pattern ASCII_PATTERN = Pattern.compile("^\\p{ASCII}*$");

        @Override
        protected String format(String value) {
          return value;
        }

        @Override
        protected String parse(String value) {
          if (!ASCII_PATTERN.matcher(value).matches()) {
            throw new CoercingParseLiteralException("String contains non-ASCII characters");
          }
          return value;
        }
      };

  /**
   * We only allow strings for Decimal -- floating-point literals could introduce precision issues.
   */
  static final StringCoercing<BigDecimal> DECIMAL =
      new StringCoercing<BigDecimal>() {
        @Override
        protected String format(BigDecimal value) {
          return value.toString();
        }

        @Override
        protected BigDecimal parse(String value) {
          return new BigDecimal(value);
        }
      };

  static final StringCoercing<ByteBuffer> BLOB =
      new StringCoercing<ByteBuffer>() {
        @Override
        protected String format(ByteBuffer value) {
          return ByteBufferUtils.toBase64(value);
        }

        @Override
        protected ByteBuffer parse(String value) {
          return ByteBufferUtils.fromBase64(value);
        }
      };

  static final StringCoercing<CqlDuration> DURATION =
      new StringCoercing<CqlDuration>() {
        @Override
        protected String format(CqlDuration value) {
          return value.toString();
        }

        @Override
        protected CqlDuration parse(String value) {
          return CqlDuration.from(value);
        }
      };
}
