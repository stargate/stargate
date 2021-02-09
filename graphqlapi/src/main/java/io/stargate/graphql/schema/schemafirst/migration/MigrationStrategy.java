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
package io.stargate.graphql.schema.schemafirst.migration;

import io.stargate.graphql.schema.schemafirst.AdminSchemaBuilder;

/** @see AdminSchemaBuilder#MIGRATION_STRATEGY_ENUM */
public enum MigrationStrategy {
  USE_EXISTING,
  ADD_MISSING_TABLES,
  ADD_MISSING_TABLES_AND_COLUMNS,
  DROP_AND_RECREATE_ALL,
  DROP_AND_RECREATE_IF_MISMATCH,
  ;
}
