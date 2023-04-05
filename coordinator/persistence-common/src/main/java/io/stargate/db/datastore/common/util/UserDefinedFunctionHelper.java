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
package io.stargate.db.datastore.common.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class UserDefinedFunctionHelper {

  /**
   * Fixes the classloader that Cassandra uses to compile UDFs.
   *
   * <p>By default, it is set to the context classloader, which works in regular Cassandra, but not
   * in Stargate's OSGi environment. This method uses reflection calls to change it to the
   * classloader that loaded Cassandra classes.
   *
   * <p>Note that <a
   * href="https://issues.apache.org/jira/browse/CASSANDRA-17013">CASSANDRA-17013</a> fixes this
   * issue directly in the Cassandra codebase; therefore this method will become obsolete once
   * Stargate depends on Cassandra JARs that have the fix.
   */
  public static void fixCompilerClassLoader() {
    try {
      // Note that we assume the same class/field names for all Cassandra/DSE versions, which is
      // currently the case.
      Class<?> targetClass =
          Class.forName("org.apache.cassandra.cql3.functions.UDFunction$UDFClassLoader");
      Field targetField = targetClass.getDeclaredField("insecureClassLoader");
      targetField.setAccessible(true);

      Field modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(targetField, targetField.getModifiers() & ~Modifier.FINAL);

      targetField.set(null, targetClass.getClassLoader());
    } catch (Exception e) {
      throw new RuntimeException(
          "Error during initialization of the persistence layer: some "
              + "reflection-based accesses cannot be setup.",
          e);
    }
  }
}
