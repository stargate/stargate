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

public class AuthenticationException extends RequestValidationException {
  public AuthenticationException(String msg) {
    super(ExceptionCode.BAD_CREDENTIALS, msg);
  }

  public AuthenticationException(Throwable e) {
    this(e.getMessage(), e);
  }

  public AuthenticationException(String msg, Throwable e) {
    super(ExceptionCode.BAD_CREDENTIALS, msg, removeStackTracesRecursively(e));
  }

  /** Information may be leaked via stack trace, so we remove them. */
  static Throwable removeStackTracesRecursively(Throwable cause) {
    for (Throwable t = cause; t != null; t = t.getCause()) {
      t.setStackTrace(new StackTraceElement[0]);
    }
    return cause;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
