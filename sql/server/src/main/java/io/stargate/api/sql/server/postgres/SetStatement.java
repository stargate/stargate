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

import io.reactivex.Flowable;
import io.stargate.api.sql.server.postgres.msg.Bind;
import io.stargate.api.sql.server.postgres.msg.CommandComplete;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import io.stargate.api.sql.server.postgres.msg.ParameterStatus;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SetStatement extends Statement {

  private static final Pattern SET_COMMAND =
      Pattern.compile("(?i)SET\\s+(\\S+)\\s*(?:=|TO)\\s*(.+)");

  private final String key;
  private final String value;

  public SetStatement(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public static SetStatement parse(String sql) {
    Matcher matcher = SET_COMMAND.matcher(sql);
    if (!matcher.matches()) {
      return null;
    }

    String key = matcher.group(1);
    String value = matcher.group(2);
    return new SetStatement(key, value);
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public Portal bind(Bind bind) {
    return new SetPortal(bind);
  }

  @Override
  public String toString() {
    return "SET " + key + " = " + value;
  }

  private class SetPortal extends Portal {

    private SetPortal(Bind bind) {
      super(SetStatement.this, bind);
    }

    @Override
    protected boolean hasResultSet() {
      return false;
    }

    @Override
    public Flowable<PGServerMessage> execute(Connection connection) {
      connection.setProperty(key, value);

      return Flowable.just(ParameterStatus.of(key, value), CommandComplete.forSet());
    }
  }
}
