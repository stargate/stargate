/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.exceptions;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;

public class WriteTimeoutException extends RequestTimeoutException {
  public final WriteType writeType;

  /**
   * @param writeType the type of the write for which the timeout was raised.
   * @param consistency the requested consistency level.
   * @param received the number of replica that had acknowledged/responded to the operation before
   *     it failed.
   * @param blockFor the minimum number of replica acknowledgements/responses that were required to
   *     fulfill the operation.
   */
  public WriteTimeoutException(
      WriteType writeType, ConsistencyLevel consistency, int received, int blockFor) {
    super(ExceptionCode.WRITE_TIMEOUT, consistency, received, blockFor);
    this.writeType = writeType;
  }

  public WriteTimeoutException(
      WriteType writeType, ConsistencyLevel consistency, int received, int blockFor, String msg) {
    super(ExceptionCode.WRITE_TIMEOUT, consistency, received, blockFor, msg);
    this.writeType = writeType;
  }
}
