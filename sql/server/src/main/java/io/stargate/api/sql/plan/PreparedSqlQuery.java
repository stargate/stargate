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
package io.stargate.api.sql.plan;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.api.sql.plan.exec.RuntimeContext;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/** Represents a prepared SQL query optimized for execution against a Cassandra storage cluster. */
public class PreparedSqlQuery {
  private final RelRoot root;
  private final EnumerableRel plan;
  private final Bindable<Object> bindable;
  private final RuntimeContext context;
  private final List<Column> resultSetColumns;
  private final RelDataType parametersType;
  private final SqlExplain explain;
  private final RelDataType resultType;
  private final JavaTypeFactory typeFactory;
  private final boolean isDml;

  public PreparedSqlQuery(
      RelDataType resultType,
      RelNode plan,
      RelRoot relRoot,
      Bindable<Object> bindable,
      RuntimeContext context,
      JavaTypeFactory typeFactory,
      boolean isDml,
      RelDataType parametersType,
      SqlExplain explain) {
    this.plan = (EnumerableRel) plan;
    this.resultType = resultType;
    this.root = relRoot;
    this.typeFactory = typeFactory;
    this.isDml = isDml;
    this.parametersType = parametersType;
    this.explain = explain;
    this.resultSetColumns = toColumns(this.resultType);
    this.bindable = bindable;
    this.context = context;
  }

  private List<Column> toColumns(RelDataType type) {
    List<RelDataTypeField> fields = type.getFieldList();

    return fields.stream()
        .map(
            f ->
                ImmutableColumn.builder()
                    .keyspace("[query]")
                    .table("[result_rows]")
                    .name(f.getName())
                    .type(TypeUtils.fromSqlType(f.getType().getSqlTypeName()))
                    .build())
        .collect(Collectors.toList());
  }

  public RelDataType getResultType() {
    if (explain == null) {
      return resultType;
    } else {
      return typeFactory.createStructType(
          Collections.singletonList(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
          Collections.singletonList("plan"));
    }
  }

  public RelDataType getParametersType() {
    return parametersType;
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public List<Column> resultSetColumns() {
    return resultSetColumns;
  }

  public boolean isDml() {
    return isDml;
  }

  public SqlKind kind() {
    return root.kind;
  }

  public Iterable<Object> execute(List<?> parameters) {
    if (explain != null) {
      return Linq4j.asEnumerable(Collections.singletonList(explain(explain)));
    }

    RuntimeContext dataContext = context.withPositionalParameters(parameters);
    return bindable.bind(dataContext);
  }

  private String explain(SqlExplain explain) {
    switch (explain.getDepth()) {
      case TYPE:
        return RelOptUtil.dumpType(resultType);
      case LOGICAL:
        return explain(explain.getFormat(), explain.getDetailLevel(), root.rel);
      case PHYSICAL:
        return explain(explain.getFormat(), explain.getDetailLevel(), plan);
      default:
        throw new IllegalArgumentException("Unsupported explain depth: " + explain.getDepth());
    }
  }

  private String explain(SqlExplainFormat format, SqlExplainLevel detailLevel, RelNode plan) {
    return RelOptUtil.dumpPlan("", plan, format, detailLevel).trim();
  }

  @VisibleForTesting
  String explain() {
    return explain(SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES, plan);
  }
}
