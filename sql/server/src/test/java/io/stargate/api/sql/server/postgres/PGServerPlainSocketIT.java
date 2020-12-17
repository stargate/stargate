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
package io.stargate.api.sql.server.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.stargate.api.sql.server.postgres.msg.StartupMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(300)
class PGServerPlainSocketIT extends PGServerTestBase {

  private final Collection<DB> connections = new ArrayList<>();

  private DB openConnection() throws IOException {
    DB c = new DB();
    connections.add(c);
    login(c);
    return c;
  }

  @AfterEach
  public void closeConnections() throws IOException {
    for (DB connection : connections) {
      connection.close();
    }

    connections.clear();
  }

  private void login(DB c) throws IOException {
    // send StartupMessage
    byte[] userKey = "user".getBytes(StandardCharsets.UTF_8);
    byte[] userName = "test".getBytes(StandardCharsets.UTF_8);

    int size = 4 + 4 + userKey.length + 1 + userName.length + 1 + 1;

    c.out.writeInt(size);
    c.out.writeInt((int) StartupMessage.PROTOCOL_CURRENT);
    c.writeString(userKey);
    c.writeString(userName);
    c.out.writeByte(0); // key-value pairs terminator
    c.out.flush();

    // expect AuthenticationOk response
    Assertions.assertThat(c.in.readByte()).isEqualTo((byte) 'R');
    Assertions.assertThat(c.in.readInt()).isEqualTo(8); // msg size
    Assertions.assertThat(c.in.readInt()).isEqualTo(0); // auth success

    Pair<String, String> serverVersion = readParameter(c);
    assertThat(serverVersion.getValue0()).isEqualTo("server_version");
    assertThat(serverVersion.getValue1()).isEqualTo("13.0");

    Pair<String, String> serverEncoding = readParameter(c);
    assertThat(serverEncoding.getValue0()).isEqualTo("server_encoding");
    assertThat(serverEncoding.getValue1()).isEqualTo("UTF8");

    Pair<String, String> timeZone = readParameter(c);
    assertThat(timeZone.getValue0()).isEqualTo("TimeZone");
    assertThat(timeZone.getValue1()).isEqualTo("GMT");

    Pair<String, String> clientEncoding = readParameter(c);
    assertThat(clientEncoding.getValue0()).isEqualTo("client_encoding");
    assertThat(clientEncoding.getValue1()).isEqualTo("UTF8");

    Map<String, String> notice = readNotice(c, 'N');
    assertThat(notice).containsEntry("S", "WARNING");
    assertThat(notice).extracting("M").asString().contains("Stargate").contains("experimental");

    readReadyForQuery(c);
  }

  private Map<String, String> readNotice(DB c, char type) throws IOException {
    readExpectedMsgType(c, type);
    int size = c.in.readInt();
    return readNoticeBody(c, size);
  }

  private static Map<String, String> readNoticeBody(DB c, int maxSize) throws IOException {
    Map<String, String> data = new HashMap<>();
    byte b;
    while ((b = c.in.readByte()) != 0) {
      char code = (char) b;
      String value = c.readString(maxSize);
      data.put("" + code, value);
    }

    return data;
  }

  private Pair<String, String> readParameter(DB c) throws IOException {
    readExpectedMsgType(c, 'S');
    int size = c.in.readInt();
    String paramName = c.readString(size);
    String paramValue = c.readString(size);
    return Pair.with(paramName, paramValue);
  }

  private void writeQuery(DB c, String sql) throws IOException {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
    c.out.writeByte('Q');
    c.out.writeInt(4 + sqlBytes.length + 1);
    c.writeString(sqlBytes);
    c.out.flush();
  }

  private static void handleError(DB c) throws IOException {
    int size = c.in.readInt();
    Map<String, String> notice = readNoticeBody(c, size);
    fail("Unexpected error from server: " + notice);
  }

  private static void readExpectedMsgType(DB c, char expected) throws IOException {
    char type = (char) c.in.readByte();
    if (type == 'E') { // error response
      handleError(c);
    } else if (type != expected) {
      fail(
          String.format(
              "Expected message type '%c', received '%c' (%d)", expected, type, (int) type));
    }
  }

  private String readCommandComplete(DB c) throws IOException {
    readExpectedMsgType(c, 'C');
    int size = c.in.readInt();
    return c.readString(size);
  }

  private void readReadyForQuery(DB c) throws IOException {
    readExpectedMsgType(c, 'Z');
    Assertions.assertThat(c.in.readInt()).isEqualTo(5); // msg size
    Assertions.assertThat(c.in.readByte()).isEqualTo((byte) 'I'); // idle = no transaction
  }

  private List<byte[]> readDataRow(DB c) throws IOException {
    readExpectedMsgType(c, 'D');
    c.in.readInt(); // message size, not used here
    int numValues = c.in.readUnsignedShort();

    List<byte[]> values = new ArrayList<>(numValues);
    for (int i = 0; i < numValues; i++) {
      int valLength = c.in.readInt();
      byte[] value = new byte[valLength];
      c.in.readFully(value);
      values.add(value);
    }

    return values;
  }

  @Test
  public void connect() throws IOException {
    DB connection = openConnection();
    assertThat(connection).isNotNull();
    connection.close();
  }

  @Test
  public void emptyQuery() throws IOException {
    withQuery(table1, "SELECT a FROM test_ks.test1").returningNothing();

    DB c = openConnection();
    writeQuery(c, "select * from test_ks.test1");

    RowDescription d = RowDescription.read(c);
    assertThat(d.names.length).isEqualTo(1);
    assertThat(d.names[0]).isEqualTo("a");

    assertThat(readCommandComplete(c)).isEqualTo("SELECT 0");
    readReadyForQuery(c);
  }

  @Test
  public void setQuery() throws IOException {
    DB c = openConnection();
    writeQuery(c, "set application_name = test123");

    Pair<String, String> param = readParameter(c);
    assertThat(param.getValue0()).isEqualTo("application_name");
    assertThat(param.getValue1()).isEqualTo("test123");

    assertThat(readCommandComplete(c)).isEqualTo("SET");
    readReadyForQuery(c);
  }

  private Object parseValue(int typeOid, short format, byte[] value) {
    assertThat(format).isEqualTo((short) 0); // text
    String strVal = new String(value, StandardCharsets.UTF_8);

    PGType type = PGType.of(typeOid);

    switch (type) {
      case Int4:
        return Integer.parseInt(strVal);
      case Varchar:
        return strVal;
    }

    throw new IllegalStateException("Unhandled type: " + type);
  }

  private void assertRows(DB c, List<Map<String, Object>> rows) throws IOException {
    RowDescription d = RowDescription.read(c);
    assertThat(d.names.length).isGreaterThanOrEqualTo(1);
    for (Map<String, Object> row : rows) {
      List<byte[]> values = readDataRow(c);
      assertThat(values.size()).isEqualTo(d.names.length);

      Builder<Object, Object> actual = ImmutableMap.builder();
      for (int i = 0; i < d.names.length; i++) {
        String name = d.names[i];
        byte[] value = values.get(i);
        Object parsed = parseValue(d.dataTypes[i], d.formats[i], value);
        actual.put(name, parsed);
      }

      assertThat(actual.build()).isEqualTo(row);
    }
  }

  @Test
  public void simpleQuery() throws IOException {
    ImmutableList<Map<String, Object>> rows =
        ImmutableList.of(ImmutableMap.of("a", 11), ImmutableMap.of("a", 22));
    withQuery(table1, "SELECT a FROM test_ks.test1").returning(rows);

    DB c = openConnection();
    writeQuery(c, "select * from test_ks.test1");

    assertRows(c, rows);
    assertThat(readCommandComplete(c)).isEqualTo("SELECT 2");
    readReadyForQuery(c);
  }

  @Test
  public void longResultSet() throws IOException {
    ImmutableList.Builder<Map<String, Object>> expected = ImmutableList.builder();
    int N = 1000;
    for (int i = 0; i < N; i++) {
      expected.add(ImmutableMap.of("a", 123));
    }

    ImmutableList<Map<String, Object>> rows = expected.build();
    withQuery(table1, "SELECT a FROM test_ks.test1").returning(rows);

    DB c = openConnection();
    writeQuery(c, "select * from test_ks.test1");

    RowDescription d = RowDescription.read(c);
    assertThat(d.names.length).isGreaterThanOrEqualTo(1);

    for (int i = 0; i < N; i++) {
      List<byte[]> values = readDataRow(c);
      assertThat(values.size()).isEqualTo(d.names.length);
    }

    assertThat(readCommandComplete(c)).isEqualTo("SELECT " + N);
    readReadyForQuery(c);
  }

  private static class DB {

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public DB() throws IOException {
      socket = new Socket("localhost", TEST_PORT);
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
    }

    public void close() throws IOException {
      socket.close();
    }

    private void writeString(byte[] value) throws IOException {
      out.write(value);
      out.writeByte(0);
    }

    private String readString(int maxSize) throws IOException {
      assertThat(maxSize).isLessThan(10_000);

      byte[] data = new byte[maxSize];
      int size = 0;

      byte b;
      while ((b = in.readByte()) != 0) {
        data[size++] = b;
      }

      return new String(data, 0, size, StandardCharsets.UTF_8);
    }
  }

  private static class RowDescription {

    private final String[] names;
    private final int[] dataTypes;
    private final short[] formats;

    public RowDescription(String[] names, int[] dataTypes, short[] formats) {
      this.names = names;
      this.dataTypes = dataTypes;
      this.formats = formats;
    }

    private static RowDescription read(DB c) throws IOException {
      readExpectedMsgType(c, 'T');
      int size = c.in.readInt();
      int fieldCount = c.in.readUnsignedShort();
      String[] names = new String[fieldCount];
      int[] dataTypes = new int[fieldCount];
      short[] formats = new short[fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        names[i] = c.readString(size);
        c.in.readInt(); // table ID
        c.in.readShort(); // column ID
        dataTypes[i] = c.in.readInt();
        c.in.readShort(); // type length
        c.in.readInt(); // type modifier
        formats[i] = c.in.readShort();
      }

      return new RowDescription(names, dataTypes, formats);
    }
  }
}
