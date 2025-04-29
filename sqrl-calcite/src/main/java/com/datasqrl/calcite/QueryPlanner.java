package com.datasqrl.calcite;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.SqrlRexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqrlRelDecorrelator;
import org.apache.calcite.sql2rel.SqrlSqlToRelConverter;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.javac.JaninoCompiler;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;

import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlConverterFactory;
import com.datasqrl.calcite.convert.SqlNodeToString.SqlStrings;
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.calcite.schema.ExpandTableMacroRule.ExpandTableMacroConfig;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.util.DataContextImpl;

import lombok.Getter;
import lombok.SneakyThrows;

/**
 * A facade to the calcite planner
 */
@Getter
public class QueryPlanner {

  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final CatalogReader catalogReader;
  private final OperatorTable operatorTable;
  private final SqrlSchema schema;
  private final ConvertletTable convertletTable;
  private final File defaultClassDir;
  private final HintStrategyTable hintStrategyTable;
  private final RelMetadataProvider metadataProvider;
  private final SqrlFramework framework;

  private final CalciteParser flinkParser;
  public QueryPlanner(SqrlFramework framework) {
    this.framework = framework;
    this.catalogReader = framework.getCatalogReader();
    this.operatorTable = framework.getSqrlOperatorTable();
    this.schema = framework.getSchema();
    this.metadataProvider = framework.getRelMetadataProvider();
    this.hintStrategyTable = framework.getHintStrategyTable();
    this.planner = new VolcanoPlanner(null, Contexts.empty());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    PlannerRules.registerDefaultRules(planner);
    EnumerableRules.rules().forEach(planner::addRule);

    RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);

    this.cluster = RelOptCluster.create(planner, new SqrlRexBuilder(framework.getTypeFactory()));
    this.convertletTable = new ConvertletTable();
    this.defaultClassDir = new File("build/calcite/classes");
    cluster.setMetadataProvider(this.metadataProvider);
    cluster.setHintStrategies(hintStrategyTable);

    var config = SqlParser.config()
        .withParserFactory(FlinkSqlParserFactories.create(FlinkSqlConformance.DEFAULT))
        .withConformance(FlinkSqlConformance.DEFAULT)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);
    this.flinkParser = new CalciteParser(config);
  }


  @SneakyThrows
  public SqlNode parse(Dialect dialect, String sql) {
    switch (dialect) {
      case POSTGRES:
      case CALCITE:
        return org.apache.calcite.sql.parser.SqlParser.create(sql,
                SqrlConfigurations.calciteParserConfig)
            .parseQuery();
      case FLINK:
        return flinkParser.parse(sql);
      default:
        throw new RuntimeException("Unknown dialect");
    }
  }
  
  /**
   * Helper function to parse a data type from a string using calcite's full
   * type definition syntax.
   */
  @SneakyThrows
  public RelDataType parseDatatype(String datatype) {
    // Must be flink types
    datatype = datatype.replaceAll("TIMESTAMP_WITH_LOCAL_TIME_ZONE",
        "TIMESTAMP_LTZ");

    // Addl type aliases for graphql
    if (datatype.equalsIgnoreCase("datetime")) {
      return this.cluster.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
    } else if (datatype.equalsIgnoreCase(CustomScalars.GRAPHQL_BIGINTEGER.getName())) {
        return this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    if (datatype.equalsIgnoreCase(CustomScalars.GRAPHQL_BIGINTEGER.getName())) {
      return this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    // For other types, delegate to Flink planner to create Calcite types
    String create = String.format("CREATE TABLE x (col %s)", datatype);
    SqlCreateTable parse = (SqlCreateTable)parse(Dialect.FLINK, create);
    SqlDataTypeSpec typeSpec = ((SqlRegularColumn) parse.getColumnList().get(0)).getType();

    return typeSpec.deriveType(createSqlValidator());
  }

  public RelNode planQueryOnTempTable(RelDataType tempSchema, String name, Consumer<RelBuilder> buildQuery) {
    try {
      schema.add(name, new TemporaryViewTable(tempSchema));

      var builder = getRelBuilder();

      buildQuery.accept(builder);

      return builder.build();
    } finally {
      schema.removeTable(name);
    }
  }
  
  public static SqlNodes relToSql(Dialect dialect, RelNode relNode) {
    var relToSql = SqlConverterFactory.get(dialect);
    return relToSql.convert(relNode);
  }

  /* Factories */
  public SqlValidator createSqlValidator() {
    return new SqrlSqlValidator(
        this.operatorTable,
        catalogReader,
        framework.getTypeFactory(),
        SqrlConfigurations.sqlValidatorConfig);
  }

  public RelBuilder getRelBuilder() {
    return new RelBuilder(null, this.cluster, this.catalogReader){};
  }

  public static SqlStrings sqlToString(Dialect dialect, SqlNodes node) {
    var sqlToString = SqlToStringFactory.get(dialect);
    return sqlToString.convert(node);
  }

  
  public static SqlStrings relToString(Dialect dialect, RelNode relNode) {
    return sqlToString(dialect, relToSql(dialect, relNode));
  }

}
