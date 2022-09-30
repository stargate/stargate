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
package io.stargate.graphql.web.resources;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.stargate.db.EventListener;
import java.util.List;

/**
 * A schema event listener that always performed the same action when a keyspace changed, regardless
 * of the nature of the change.
 */
interface KeyspaceChangeListener extends EventListener {

  @FormatMethod
  void onKeyspaceChanged(
      String keyspaceName, @FormatString String reason, Object... reasonArguments);

  @Override
  default void onDropKeyspace(String keyspaceName) {
    onKeyspaceChanged(keyspaceName, "it was dropped");
  }

  @Override
  default void onCreateTable(String keyspaceName, String table) {
    onKeyspaceChanged(keyspaceName, "table %s was created", table);
  }

  @Override
  default void onCreateView(String keyspaceName, String view) {
    onKeyspaceChanged(keyspaceName, "view %s was created", view);
  }

  @Override
  default void onCreateType(String keyspaceName, String type) {
    onKeyspaceChanged(keyspaceName, "type %s was created", type);
  }

  @Override
  default void onCreateFunction(String keyspaceName, String function, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "function %s was created", function);
  }

  @Override
  default void onCreateAggregate(
      String keyspaceName, String aggregate, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "aggregate %s was created", aggregate);
  }

  @Override
  default void onAlterTable(String keyspaceName, String table) {
    onKeyspaceChanged(keyspaceName, "table %s was altered", table);
  }

  @Override
  default void onAlterView(String keyspaceName, String view) {
    onKeyspaceChanged(keyspaceName, "view %s was altered", view);
  }

  @Override
  default void onAlterType(String keyspaceName, String type) {
    onKeyspaceChanged(keyspaceName, "type %s was altered", type);
  }

  @Override
  default void onAlterFunction(String keyspaceName, String function, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "function %s was altered", function);
  }

  @Override
  default void onAlterAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "aggregate %s was altered", aggregate);
  }

  @Override
  default void onDropTable(String keyspaceName, String table) {
    onKeyspaceChanged(keyspaceName, "table %s was dropped", table);
  }

  @Override
  default void onDropView(String keyspaceName, String view) {
    onKeyspaceChanged(keyspaceName, "view %s was dropped", view);
  }

  @Override
  default void onDropType(String keyspaceName, String type) {
    onKeyspaceChanged(keyspaceName, "type %s was dropped", type);
  }

  @Override
  default void onDropFunction(String keyspaceName, String function, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "function %s was dropped", function);
  }

  @Override
  default void onDropAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    onKeyspaceChanged(keyspaceName, "aggregate %s was dropped", aggregate);
  }
}
