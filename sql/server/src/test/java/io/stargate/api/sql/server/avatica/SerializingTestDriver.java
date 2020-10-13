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
package io.stargate.api.sql.server.avatica;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.KerberosConnection;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufHandler;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;

public class SerializingTestDriver extends Driver {

  public static final String JDBC_URL = "jdbc:stargate:test:serialization:";

  private static final String HANDLER = "stargate.test.driver.handler";

  static {
    try {
      DriverManager.registerDriver(new SerializingTestDriver());
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  static final DriverVersion VERSION =
      new DriverVersion(
          "Stargate Test JDBC Driver that emulates remote protocol without network access",
          "1.0.0",
          "Stargate",
          "1.0.0",
          true,
          1,
          0,
          1,
          0);

  public static Connection newConnection(
      SerializationParams params, StargateMeta meta, String user, String password)
      throws SQLException {
    Properties connectionProperties = new Properties();
    if (user != null) connectionProperties.put("user", user);
    if (password != null) connectionProperties.put("password", password);

    Service service = new LocalService(meta);
    connectionProperties.put(HANDLER, params.handlerFactory.apply(service));

    connectionProperties.put(
        BuiltInConnectionProperty.HTTP_CLIENT_FACTORY.camelName(),
        TestClientFactory.class.getName());
    connectionProperties.put(
        BuiltInConnectionProperty.URL.camelName(), "http://localhost/stargate/test");
    connectionProperties.put(
        BuiltInConnectionProperty.SERIALIZATION.camelName(), params.serialization.name());

    return DriverManager.getConnection(SerializingTestDriver.JDBC_URL, connectionProperties);
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return VERSION;
  }

  @Override
  protected String getConnectStringPrefix() {
    return JDBC_URL;
  }

  public static final class TestClientFactory implements AvaticaHttpClientFactory {
    @Override
    public AvaticaHttpClient getClient(
        URL url, ConnectionConfig config, KerberosConnection kerberosUtil) {
      Function<byte[], byte[]> handler;
      try {
        Field properties = config.getClass().getDeclaredField("properties");
        properties.setAccessible(true);
        Properties p = (Properties) properties.get(config);
        //noinspection unchecked
        handler = (Function<byte[], byte[]>) p.get(HANDLER);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

      return handler::apply;
    }
  }

  private static final class LocalProtobufHandler implements Function<byte[], byte[]> {
    private final ProtobufHandler handler;

    private LocalProtobufHandler(Service service) {
      this.handler =
          new ProtobufHandler(
              service, new ProtobufTranslationImpl(), NoopMetricsSystem.getInstance());
    }

    @Override
    public byte[] apply(byte[] bytes) {
      return handler.apply(bytes).getResponse();
    }
  }

  private static final class LocalJsonHandler implements Function<byte[], byte[]> {
    private final JsonHandler handler;

    private LocalJsonHandler(Service service) {
      this.handler = new JsonHandler(service, NoopMetricsSystem.getInstance());
    }

    @Override
    public byte[] apply(byte[] bytes) {
      String input = new String(bytes);
      return handler.apply(input).getResponse().getBytes();
    }
  }

  public enum SerializationParams {
    PROTOBUF(Serialization.PROTOBUF, LocalProtobufHandler::new, v -> v),
    JSON(
        Serialization.JSON,
        LocalJsonHandler::new,
        v -> {
          // byte, short and int values are indistinguishable when serialized by Avatica as JSON
          if (v instanceof Byte) return ((Number) v).intValue();
          if (v instanceof Short) return ((Number) v).intValue();
          // double values are represented as BigDecimal
          if (v instanceof Double) return BigDecimal.valueOf((Double) v);
          return v;
        }),
    ;

    private final Serialization serialization;
    private final Function<Service, Function<byte[], byte[]>> handlerFactory;
    private final Function<Object, Object> coercer;

    SerializationParams(
        Serialization serialization,
        Function<Service, Function<byte[], byte[]>> handlerFactory,
        Function<Object, Object> coercer) {
      this.serialization = serialization;
      this.handlerFactory = handlerFactory;
      this.coercer = coercer;
    }

    public Object coerceClientValue(Object expectedValue) {
      return coercer.apply(expectedValue);
    }
  }
}
