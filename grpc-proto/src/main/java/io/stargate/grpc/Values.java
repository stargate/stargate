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
package io.stargate.grpc;

import com.google.protobuf.ByteString;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.UdtValue;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class Values {
  public static Value NULL = Value.newBuilder().setNull(Null.newBuilder().build()).build();
  public static Value UNSET = Value.newBuilder().setUnset(Unset.newBuilder().build()).build();

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

  public static Value of(boolean value) {
    return Value.newBuilder().setBoolean(value).build();
  }

  public static Value of(LocalDate value) {
    long days = ChronoUnit.DAYS.between(EPOCH, value);
    int unsigned = (int) days - Integer.MIN_VALUE;
    return Value.newBuilder().setDate(unsigned).build();
  }

  public static Value of(float value) {
    return Value.newBuilder().setFloat(value).build();
  }

  public static Value of(double value) {
    return Value.newBuilder().setDouble(value).build();
  }

  public static Value of(byte[] value) {
    return Value.newBuilder().setBytes(ByteString.copyFrom(value)).build();
  }

  public static Value of(InetAddress value) {
    return of(value.getAddress());
  }

  public static Value of(long value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value of(String value) {
    return Value.newBuilder().setString(value).build();
  }

  public static Value of(LocalTime value) {
    return Value.newBuilder().setTime(value.toNanoOfDay()).build();
  }

  public static Value of(UUID value) {
    return Value.newBuilder()
        .setUuid(
            Uuid.newBuilder()
                .setMsb(value.getMostSignificantBits())
                .setLsb(value.getLeastSignificantBits()))
        .build();
  }

  public static Value of(Value... elements) {
    return Value.newBuilder()
        .setCollection(Collection.newBuilder().addAllElements(Arrays.asList(elements)).build())
        .build();
  }

  public static Value udtOf(Map<String, Value> fields) {
    return Value.newBuilder().setUdt(UdtValue.newBuilder().putAllFields(fields).build()).build();
  }
}
