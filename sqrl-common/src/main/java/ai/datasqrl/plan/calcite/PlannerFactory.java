package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.time.FlinkFnc;
import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

@AllArgsConstructor
public class PlannerFactory {
  private final SchemaPlus rootSchema;

  public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(false)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(true) //must be true to allow null literals
      .withLenientOperatorLookup(false)
      .withSqlConformance(SqrlConformance.INSTANCE)
      ;

  public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false)
      .withHintStrategyTable(SqrlHintStrategyTable.getHintStrategyTable());


  public static SqlOperatorTable getOperatorTable() {
    return getOperatorTable(List.of());
  }
  public static SqlOperatorTable getOperatorTable(List<FlinkFnc> envFunctions) {
    return FlinkEnvProxy.getOperatorTable(envFunctions);
  }

  public static RelDataTypeFactory getTypeFactory() {
    return new FlinkTypeFactory(getTypeSystem());
  }

  public static RelDataTypeSystem getTypeSystem() {
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
