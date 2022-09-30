/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.schema;

import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class SecondaryIndex implements Index, QualifiedSchemaEntity {
  private static final long serialVersionUID = 424886903165529554L;

  @Nullable
  public abstract Column column();

  public abstract CollectionIndexingType indexingType();

  @Value.Default
  public boolean isCustom() {
    String indexingClass = indexingClass();
    return indexingClass != null && !indexingClass.isEmpty();
  }

  @Nullable
  public abstract String indexingClass();

  @Nullable
  public abstract Map<String, String> indexingOptions();

  public static SecondaryIndex create(String keyspace, String name, Column column) {
    return ImmutableSecondaryIndex.builder()
        .keyspace(keyspace)
        .name(name)
        .column(column)
        .indexingType(ImmutableCollectionIndexingType.builder().build())
        .build();
  }

  public static SecondaryIndex create(
      String keyspace,
      String name,
      Column column,
      CollectionIndexingType indexingType,
      String indexClass,
      Map<String, String> indexOptions) {
    return ImmutableSecondaryIndex.builder()
        .keyspace(keyspace)
        .name(name)
        .column(column)
        .indexingType(indexingType)
        .indexingClass(indexClass)
        .indexingOptions(indexOptions)
        .build();
  }

  public static SecondaryIndex reference(String name) {
    return ImmutableSecondaryIndex.builder()
        .keyspace("ignored-maybe")
        .name(name)
        .indexingType(ImmutableCollectionIndexingType.builder().build())
        .build();
  }

  @Override
  public int priority() {
    return 2;
  }

  @Override
  public String indexTypeName() {
    return "Secondary index";
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  public int schemaHashCode() {
    return SchemaHashable.combine(
        SchemaHashable.hashCode(keyspace()),
        SchemaHashable.hashCode(name()),
        SchemaHashable.hashCode(column()),
        SchemaHashable.hashCode(indexingType()),
        SchemaHashable.hashCode(isCustom()),
        SchemaHashable.hashCode(indexingClass()),
        SchemaHashable.hashCode(indexingOptions()),
        SchemaHashable.hashCode(priority()),
        SchemaHashable.hashCode(indexTypeName()));
  }
}
