package org.apache.calcite.jdbc;

import ai.dataeng.sqml.analyzer2.Analyzer2;
import ai.dataeng.sqml.analyzer2.ImportStub;
import ai.dataeng.sqml.analyzer2.TableManager;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.tree.Script;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder;
import org.junit.jupiter.api.Test;

public class CalciteTest {

  @Test
  @SneakyThrows
  public void testSqlToSqlNodes() {

    SqrlSchema schema = new SqrlSchema();
    SimpleCalciteSchema calciteSchema = new SimpleCalciteSchema(null, schema, "");
    SqlConformance conformance = FlinkSqlConformance.DEFAULT;
    Config parserConfig = SqlParser.config()
        .withParserFactory(FlinkSqlParserImpl.FACTORY)
        .withConformance(conformance)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);

    RelDataTypeSystem typeSystem = new FlinkTypeSystem();
    FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);

    Program p = Programs.of(RuleSets.ofList(
        CoreRules.SORT_PROJECT_TRANSPOSE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE));
    Program program = Programs.ofRules(
        new SortRemoveRule2(SortRemoveRule2.Config.DEFAULT)
        );
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(calciteSchema.plus())
        .parserConfig(parserConfig)
//        .costFactory(new FlinkCostFactory())
        .typeSystem(typeSystem)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
//        .sqlToRelConverterConfig(getSqlToRelConverterConfig(getCalciteConfig(tableConfig)))
//        .operatorTable(getSqlOperatorTable(getCalciteConfig(tableConfig)))
        // set the executor to evaluate constant expressions
//        .executor(new ExpressionReducer(tableConfig, false))
//        .context(context)
        .programs(program)
//        .traitDefs(traitDefs)
        .build();

///////////

    //////////////

    Planner planner = Frameworks.getPlanner(config);
    //TODO: Need to replace paths with ``.
    SqlNode parse = planner.parse("SELECT a FROM `@.b` ORDER BY a");
    //Extend this

    System.out.println(parse);

    SqlNode validate = planner.validate(parse);
    RelRoot planRoot = planner.rel(validate);


    HepProgram hep = HepProgram.builder()
        .addRuleClass(SortRemoveRule2.class)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hep);
    hepPlanner.addRule(new SortRemoveRule2(SortRemoveRule2.Config.DEFAULT));
    hepPlanner.setRoot(planRoot.rel);

    RelNode optimizedNode = hepPlanner.findBestExp();
    System.out.println(optimizedNode);


    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0);

    String sql = sqlNode.toSqlString(c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    System.out.println(sql);
  }


  public class ContextualSchema extends SqrlSchema {

  }

  public class SqrlTable extends AbstractTable {

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//      FlinkTypeFactory typeFactory = (FlinkTypeFactory) relDataTypeFactory;
      final RelDataTypeFactory.Builder fieldInfo = relDataTypeFactory.builder();
      fieldInfo.add("a", relDataTypeFactory.createSqlType(SqlTypeName.CHAR));
      return RelDataTypeImpl.proto(fieldInfo.build()).apply(relDataTypeFactory);
    }
  }
  public class SqrlSchema extends ASqrlSchema {

    @Override
    public Table getTable(String s) {

      return new SqrlTable();
    }

    @Override
    public Set<String> getTableNames() {
      return null;
    }

    @Override
    public Schema getSubSchema(String s) {
      //Return null for no sub schema
      //If schema starts with an '@' then return context schema, otherwise, normal schema

      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return null;
    }

    @Override
    public Expression getExpression(SchemaPlus schemaPlus, String s) {
      return null;
    }

    @Override
    public boolean isMutable() {
      return false;
    }
  }

  public abstract class ASqrlSchema implements Schema {

    @Override
    public RelProtoDataType getType(String name) {
      return null;
    }

    @Override
    public Set<String> getTypeNames() {
      return Collections.emptySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return Collections.emptyList();
    }

    @Override
    public Set<String> getFunctionNames() {
      return Collections.emptySet();
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
      return this;
    }
  }

  @Test
  @SneakyThrows
  public void testRelToSql() {
      String scriptStr = "IMPORT ecommerce-data.Orders;\n"
          + "Orders.total := SELECT sum(quantity) AS quantity\n"
          + "                FROM @.entries;"
          + "Orders.total2 := SELECT DISTINCT quantity\n"
          + "                FROM @.total;"
          + "Orders.total3 := SELECT quantity\n"
          + "                FROM @.total;";
    final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inStreamingMode()
            .build();
    final TableEnvironment env = TableEnvironment.create(settings);
//    env.getConfig().getPlannerConfig().
    env.getConfig().addConfiguration(
        new Configuration()
            .set(RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED(), true)
    );


    SqmlParser parser = SqmlParser.newSqmlParser();

    Script script = parser.parse(scriptStr);

    TableManager tableManager = new TableManager();

    new Analyzer2(script, env, tableManager, new ImportStub(env, tableManager), false)
        .analyze();

  }


}
