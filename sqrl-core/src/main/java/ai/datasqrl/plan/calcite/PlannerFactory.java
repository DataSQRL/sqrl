package ai.datasqrl.plan.calcite;

import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

@AllArgsConstructor
public class PlannerFactory {
  private final SchemaPlus rootSchema;

  public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(true)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(false)
      .withLenientOperatorLookup(true)
      .withSqlConformance(SqrlConformance.INSTANCE)
      ;

  public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false)
      .withHintStrategyTable(SqrlHintStrategyTable.getHintStrategyTable());


  public static SqlOperatorTable getOperatorTable() {
    LocalFlinkStreamEngineImpl x = new LocalFlinkStreamEngineImpl();
    TableEnvironmentImpl t = (TableEnvironmentImpl)x.createJob().getTableEnvironment();

    SqlOperatorTable operatorTable = SqlOperatorTables.chain(
        new FunctionCatalogOperatorTable(
            FlinkEnvProxy.getFunctionCatalog(t),
            t.getCatalogManager().getDataTypeFactory(),
            new FlinkTypeFactory(new FlinkTypeSystem())),
        FlinkSqlOperatorTable.instance());
    return operatorTable;

  }

  public static RelDataTypeFactory getTypeFactory() {
    return new FlinkTypeFactory(getTypeSystem());
  }

  private static RelDataTypeSystem getTypeSystem() {
    return new FlinkTypeSystem();
  }

  public Planner createPlanner() {

    FrameworkConfig config = Frameworks
        .newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT.withConformance(SqrlConformance.INSTANCE))
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .operatorTable(getOperatorTable())
        .programs(OptimizationStage.getAllPrograms())
        .typeSystem(new FlinkTypeSystem())
        .defaultSchema(rootSchema) //todo: may need proper subschema for enum
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .sqlValidatorConfig(sqlValidatorConfig)
        .build();

    return new Planner(config);
  }
}
