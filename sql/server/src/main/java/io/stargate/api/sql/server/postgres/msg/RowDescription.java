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
import io.stargate.api.sql.server.postgres.FieldInfo;
import io.stargate.api.sql.server.postgres.Portal;
import java.util.ArrayList;
import java.util.List;

public class RowDescription extends PGServerMessage {

  private final List<FieldInfo> fields;

  private RowDescription(List<FieldInfo> fields) {
    this.fields = fields;
  }

  public static RowDescription from(Portal portal) {
    return new RowDescription(portal.fields());
  }

  @Override
  public void write(ByteBuf out) {
    out.writeByte('T');

    int size = 4; // size int32
    size += 2; // field count int16

    List<PGString> names = new ArrayList<>(fields.size());
    for (FieldInfo field : fields) {
      PGString name = new PGString(field.getName());
      names.add(name);
      size += name.size();
      size += 4; // field's column ID int32
      size += 2; // field's table ID int16
      size += 4; // field's data type OID int32
      size += 2; // field's data type size int16
      size += 4; // field's type modifier int32
      size += 2; // field's format code int16
    }

    out.writeInt(size);
    out.writeShort(fields.size());

    int idx = 0;
    for (FieldInfo field : fields) {
      PGString name = names.get(idx++);
      name.write(out);

      out.writeInt(0); // column ID
      out.writeShort(0); // table ID
      out.writeInt(field.getType().oid());
      out.writeShort(field.getType().length());
      out.writeInt(-1); // type modifier
      out.writeShort(field.getFormat().code());
    }
  }
}
