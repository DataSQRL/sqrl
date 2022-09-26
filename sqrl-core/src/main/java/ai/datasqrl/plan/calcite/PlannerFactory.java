package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

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

  public Planner createPlanner() {
    FrameworkConfig config = Frameworks
        .newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT.withConformance(SqrlConformance.INSTANCE))
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .operatorTable(SqrlOperatorTable.instance())
        .programs(OptimizationStage.getAllPrograms())
        .typeSystem(SqrlTypeSystem.INSTANCE)
        .defaultSchema(rootSchema) //todo: may need proper subschema for enum
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .sqlValidatorConfig(sqlValidatorConfig)
        .build();

    return new Planner(config);
  }
}
