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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;

public abstract class PersistenceException extends RuntimeException {
  private final ExceptionCode code;
  private volatile List<String> warnings;

  protected PersistenceException(ExceptionCode code, String msg) {
    super(msg);
    this.code = code;
  }

  protected PersistenceException(ExceptionCode code, String msg, Throwable cause) {
    super(msg, cause);
    this.code = code;
  }

  public ExceptionCode code() {
    return code;
  }

  public PersistenceException setWarnings(List<String> warnings) {
    Preconditions.checkState(this.warnings == null, "Warnings have already been set");
    this.warnings = warnings;
    return this;
  }

  public @Nullable List<String> warnings() {
    return this.warnings;
  }
}
