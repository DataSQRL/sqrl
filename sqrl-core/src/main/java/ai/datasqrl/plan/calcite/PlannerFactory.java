package ai.datasqrl.plan.calcite;

import lombok.AllArgsConstructor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

@AllArgsConstructor
public class PlannerFactory {
  private final SqrlSchemaCatalog rootSchema;


  public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(true)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(false)
      .withLenientOperatorLookup(true)
      .withSqlConformance(SqrlConformance.INSTANCE);

  final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false);

  public Planner createPlanner(String schemaName) {
    FrameworkConfig config = Frameworks
        .newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT.withConformance(SqrlConformance.INSTANCE))
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .operatorTable(SqrlOperatorTable.instance())
        .programs(Rules.programs())
        .typeSystem(SqrlTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(schemaName))
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .sqlValidatorConfig(sqlValidatorConfig)
        .build();

    return new Planner(config);
  }
}
