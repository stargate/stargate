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

import com.google.common.collect.ImmutableList;
import io.stargate.api.sql.plan.exec.NoOpSparkHandler;
import io.stargate.api.sql.plan.exec.RuntimeContext;
import io.stargate.api.sql.plan.rule.FullScanRule;
import io.stargate.api.sql.plan.rule.ModificationRule;
import io.stargate.api.sql.plan.rule.PrimaryKeyQueryRule;
import io.stargate.api.sql.schema.ImmutableSqlSchema;
import io.stargate.api.sql.schema.SqlSchema;
import io.stargate.api.sql.schema.StorageTable;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Schema;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

/**
 * Stargate SQL query planner. This class is the main integration point between Stargate and Apache
 * Calcite. It drives query parsing, planning and optimization.
 */
public class QueryPlanner {

  private static final SqlParser.Config CONFIG = makeConfig();

  private static final NoOpSparkHandler spark = new NoOpSparkHandler();

  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

  private static SqlParser.Config makeConfig() {
    SqlParser.ConfigBuilder parserConfig =
        SqlParser.configBuilder()
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.TO_LOWER);
    return parserConfig.build();
  }

  public PreparedSqlQuery prepare(String sql, DataStore dataStore, String defaultKeyspace)
      throws Exception {
    CalciteSchema rootSchema = convertSchema(dataStore.schema());

    SqlParser sqlParser = SqlParser.create(sql, CONFIG);
    SqlNode sqlNode = sqlParser.parseQuery();

    VolcanoPlanner planner = new VolcanoPlanner(null, Contexts.EMPTY_CONTEXT);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    RelOptUtil.registerDefaultRules(planner, true, false);

    planner.addRule(new FullScanRule());
    planner.addRule(new ModificationRule());
    planner.addRule(new PrimaryKeyQueryRule());

    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

    SqlOperatorTable opTab = SqlStdOperatorTable.instance();

    Prepare.CatalogReader catalogReader =
        new CalciteCatalogReader(
            rootSchema,
            defaultKeyspace == null ? Collections.emptyList() : ImmutableList.of(defaultKeyspace),
            typeFactory,
            CalciteConnectionConfig.DEFAULT);

    SqlValidator validator =
        new Validator(opTab, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

    SqlToRelConverter converter =
        new SqlToRelConverter(
            new NoopViewExpander(),
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.Config.DEFAULT);

    SqlExplain explain = null;
    if (sqlNode.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) sqlNode;
      sqlNode = explain.getExplicandum();
    }

    RelRoot relRoot = converter.convertQuery(sqlNode, true, true);

    Program program = Programs.standard();

    RelTraitSet desiredTraits =
        relRoot
            .rel
            .getTraitSet()
            .replace(EnumerableConvention.INSTANCE)
            .replace(relRoot.collation)
            .simplify();

    RelNode optimized =
        program.run(
            planner,
            relRoot.rel,
            desiredTraits,
            planner.getMaterializations(),
            Collections.emptyList());

    EnumerableRel plan = (EnumerableRel) optimized;

    RelDataType resultType = validator.getValidatedNodeType(sqlNode);

    Map<String, Object> parameters = new HashMap<>();
    RuntimeContext.Builder builder = CalciteUtils.initBuilder(dataStore, parameters);

    Bindable bindable =
        EnumerableInterpretable.toBindable(parameters, spark, plan, EnumerableRel.Prefer.ARRAY);

    RelDataType parameterRowType = validator.getParameterRowType(sqlNode);

    return new PreparedSqlQuery(
        resultType,
        plan,
        relRoot,
        bindable,
        builder.build(),
        typeFactory,
        isDml(sqlNode),
        parameterRowType,
        explain);
  }

  private boolean isDml(SqlNode sqlNode) {
    switch (sqlNode.getKind()) {
      case INSERT:
      case UPDATE:
      case DELETE:
        return true;
      default:
        return false;
    }
  }

  private CalciteSchema convertSchema(Schema backendSchema) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

    backendSchema
        .keyspaces()
        .forEach(
            ks -> {
              SqlSchema schema =
                  ImmutableSqlSchema.builder()
                      .name(ks.name())
                      .tables(
                          ks.tables().stream()
                              .map(t -> StorageTable.from(ks, t))
                              .collect(Collectors.toList()))
                      .build();
              rootSchema.add(schema.name(), schema);
            });

    return rootSchema;
  }

  private static class NoopViewExpander implements RelOptTable.ViewExpander {
    @Override
    public RelRoot expandView(
        RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
      throw new UnsupportedOperationException();
    }
  }

  private static class Validator extends SqlValidatorImpl {
    protected Validator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Config config) {
      super(opTab, catalogReader, typeFactory, config);
    }
  }
}
