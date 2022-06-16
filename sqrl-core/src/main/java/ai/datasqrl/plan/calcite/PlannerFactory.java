package ai.datasqrl.plan.calcite;

import java.util.Properties;
import lombok.AllArgsConstructor;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
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
      .withSqlConformance(SqlConformanceEnum.LENIENT);

  final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false);

  public Planner createPlanner(String schemaName) {
    FrameworkConfig config = Frameworks
        .newConfigBuilder()
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .operatorTable(SqrlOperatorTable.instance())
        .programs(Rules.programs())
        .typeSystem(SqrlTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(schemaName))
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .sqlValidatorConfig(sqlValidatorConfig)
        .context(new Context()
        {
          @Override
          @SuppressWarnings("unchecked")
          public <C> C unwrap(final Class<C> aClass)
          {
            if (aClass.equals(CalciteConnectionConfig.class)) {
              final Properties props = new Properties();
              return (C) new CalciteConnectionConfigImpl(props) {
                @Override
                public SqlConformance conformance()
                {
                  return SqrlConformance.INSTANCE;
                }
              };
            } else {
              return null;
            }
          }
        })
        .build();

    return new Planner(config);
  }
}
