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
package io.stargate.api.sql.server.postgres.msg;

import io.netty.buffer.ByteBuf;
import io.reactivex.Flowable;
import io.stargate.api.sql.server.postgres.Connection;
import java.util.ArrayList;
import java.util.List;

public class Bind extends ExtendedQueryMessage {

  private final String portalName;
  private final String statementName;
  private final int[] paramFormatCodes;
  private final List<byte[]> params;
  private final int[] resultFormatCodes;

  private Bind(
      String portalName,
      String statementName,
      int[] paramFormatCodes,
      List<byte[]> params,
      int[] resultFormatCodes) {
    this.portalName = portalName;
    this.statementName = statementName;
    this.paramFormatCodes = paramFormatCodes;
    this.params = params;
    this.resultFormatCodes = resultFormatCodes;
  }

  public static Bind empty() {
    return new Bind("", "", new int[0], new ArrayList<>(0), new int[0]);
  }

  public static Bind create(int bodySize, ByteBuf bytes) {
    String portalName = readString(bytes);
    String statementName = readString(bytes);

    int numCodes = bytes.readUnsignedShort();
    int[] paramFormatCodes = new int[numCodes];
    for (int i = 0; i < paramFormatCodes.length; i++) {
      paramFormatCodes[i] = bytes.readShort();
    }

    int numParams = bytes.readUnsignedShort();

    if (numCodes > 1 && numCodes != numParams) {
      throw new IllegalArgumentException(
          String.format(
              "The number of parameters (%d) does not "
                  + "match the number of parameter format codes (%d)",
              numParams, numCodes));
    }

    ArrayList<byte[]> params = new ArrayList<>(numParams);
    while (numParams-- > 0) {
      int paramSize = bytes.readInt();
      if (paramSize < 0) {
        params.add(null);
      } else {
        byte[] value = new byte[paramSize];
        bytes.readBytes(value);
        params.add(value);
      }
    }

    int numResultCodes = bytes.readUnsignedShort();
    int[] resultFormatCodes = new int[numResultCodes];
    for (int i = 0; i < resultFormatCodes.length; i++) {
      resultFormatCodes[i] = bytes.readShort();
    }

    return new Bind(portalName, statementName, paramFormatCodes, params, resultFormatCodes);
  }

  @Override
  public Flowable<PGServerMessage> process(Connection connection) {
    return connection.bind(this);
  }

  public String getPortalName() {
    return portalName;
  }

  public String getStatementName() {
    return statementName;
  }

  public int[] getResultFormatCodes() {
    return resultFormatCodes;
  }

  public int numberOfParameters() {
    return params.size();
  }

  public int parameterFormat(int index) {
    if (paramFormatCodes.length == 0) {
      return 0; // text by default
    }

    if (paramFormatCodes.length == 1) {
      return paramFormatCodes[0];
    }

    return paramFormatCodes[index];
  }

  public byte[] parameterBytes(int index) {
    return params.get(index);
  }
}
